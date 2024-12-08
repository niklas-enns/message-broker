package niklase.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationLinks {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationLinks.class);

    private int clusterEntryLocalPort;
    private ServerSocket replicationLinkServerSocket;
    private List<Node> otherNodes = new LinkedList<>();
    private Topics topics;
    private String nodeId;

    public void startAcceptingIncomingReplicationLinkConnections(final Topics topics) {
        this.topics = topics;
        Thread.ofVirtual().start(() -> {
            logger.info("Opening cluster entry on port {}", clusterEntryLocalPort);
            try {
                replicationLinkServerSocket = new ServerSocket(clusterEntryLocalPort);
                while (true) {
                    try {
                        var socketWithOtherNode = replicationLinkServerSocket.accept();

                        Thread.ofVirtual().start(() -> {
                            logger.info("Another node connected to this one");
                            try {
                                handleIncomingMessages(socketWithOtherNode, null);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });

                    } catch (SocketException e) {
                        logger.info("Stopped processing replication requests");
                        break;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private String getNodes() {
        var string = "";
        for (final Node otherNode : this.otherNodes) {
            string += otherNode.id + "," + otherNode.getAddressForNewReplicationLinks() + ",";
        }
        return string;
    }

    public void setClusterEntryLocalPort(final int clusterEntryLocalPort) {
        this.clusterEntryLocalPort = clusterEntryLocalPort;
    }

    public void stop() throws IOException {
        this.replicationLinkServerSocket.close();
    }

    /**
     * @param line an envelope of type MESSAGE
     */
    public void acceptMessage(final String line) {
        logger.info("Replicating: {}", line);
        this.sendToReplicationReceivers(line);
    }

    /**
     * @param envelope      an envelope of type MESSAGE
     * @param consumerGroup the name of the ConsumerGroup that the message has been delivered to
     */
    public void acceptMessageDeliveryConfirmation(final String envelope, final String consumerGroup) {
        logger.info("Propagating delivery confirmation: {} in consumerGroup {} ", envelope, consumerGroup);
        var envelopeWithoutType = envelope.substring(envelope.indexOf(","));
        sendToReplicationReceivers("DELIVERED" + "," + consumerGroup + envelopeWithoutType);
    }

    public void acceptSubscriptionRequest(final String topic, final String consumerGroupName) {
        logger.info("Propagating subscription request of consumerGroup: [{}] to topic [{}]", consumerGroupName, topic);
        sendToReplicationReceivers("REPLICATED_SUBSCRIPTION_REQUEST," + topic + "," + consumerGroupName);
    }

    private void sendToReplicationReceivers(final String line) {
        logger.info("Sending {} to {}", line, otherNodes);
        this.otherNodes.forEach(node -> {
            try {
                new PrintStream(node.getSocketOfEstablishedReplicationLink().getOutputStream()).println(line);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void establishLink(final InetSocketAddress clusterEntryAddress) {
        logger.info("Establishing replication link to {}", clusterEntryAddress);
        Thread.ofVirtual().start(() -> {
            try (var socketToOtherNode = new Socket();) {
                socketToOtherNode.connect(clusterEntryAddress);
                new PrintStream(socketToOtherNode.getOutputStream()).println(
                        "INFO," + this.nodeId + ",localhost:"
                                + this.clusterEntryLocalPort); //INFO,<node id>,<cluster entry ip:port>
                handleIncomingMessages(socketToOtherNode, clusterEntryAddress);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void handleIncomingMessages(final Socket socket, final InetSocketAddress clusterEntryAddress)
            throws IOException {
        var bufferedReader = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));
        logger.info("Listening for incoming messages from other node: {}", socket);
        while (true) {
            logger.info("{} is waiting for new messages from other node", this.nodeId);
            String line = bufferedReader.readLine();
            logger.info("{} Got message from other broker: [{}]", this.nodeId, line);
            var parts = line.split(",");
            switch (parts[0]) {
            case "DELIVERED":
                topics.deleteMessage(parts[2], parts[1], parts[3]);
                break;
            case "REPLICATED_MESSAGE": // REPLICATED_MESSAGE,<consumer group name>,<topic name>,<message>
                topics.storeInConsumerGroup(parts[1], "MESSAGE," + parts[2] + "," + parts[3]);
                break;
            case "REPLICATED_SUBSCRIPTION_REQUEST":
                topics.subscribeConsumerGroupToTopic(parts[1], parts[2]);
                break;
            case "WELCOME_TO_THE_HOOD": //WELCOME_TO_THE_HOOD,<id of welcomer>,<id of other node>,<ip of other node>:<port of other node>
                this.otherNodes.add(new Node(parts[1], socket, clusterEntryAddress));
                if (parts.length >= 3) {
                    connectToUnconnectedNodes(parts[2], parts[3]);
                }
                break;
            case "INFO": //INFO,<node id>,<cluster entry ip:port>
                var addressParts = parts[2].split(":");
                var enteringNode = new Node(parts[1], socket,
                        new InetSocketAddress(addressParts[0], Integer.parseInt(addressParts[1])));

                var addresses = getNodes();
                new PrintStream(socket.getOutputStream()).println(
                        "WELCOME_TO_THE_HOOD," + this.nodeId + ","
                                + addresses); //WELCOME_TO_THE_HOOD,<id of welcomer>,<id of other node>,<ip of other node>:<port of other node>
                this.otherNodes.add(enteringNode);
                System.out.println(">> WELCOME_TO_THE_HOOD");
                break;
            default:
                logger.warn("", new IllegalArgumentException("Unknown message type in message: " + line));
            }
        }
    }

    private void connectToUnconnectedNodes(final String id, final String address) {
        logger.info("Discovered node {} at {}", id, address);
        if (!otherNodes.contains(new Node(id, null, null))) {
            var ipPortParts = address.split(":");
            establishLink(new InetSocketAddress(ipPortParts[0].split("/")[1], Integer.parseInt(ipPortParts[1])));
        } else {
            logger.info("Skipping {}, because replication link already established", id);
        }
    }

    public Set<String> getIdsOfAllnodesWithEstablishedReplicationLinks() {
        return this.otherNodes.stream()
                .map(n -> n.id)
                .collect(Collectors.toSet());
    }

    public void setNodeId(final String nodeId) {
        this.nodeId = nodeId;
    }
}
