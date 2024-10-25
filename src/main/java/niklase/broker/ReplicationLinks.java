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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationLinks {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationLinks.class);

    private int clusterEntryLocalPort;
    private ServerSocket replicationLinkServerSocket;
    private List<Socket> otherNodes = new LinkedList<>();
    private Topics topics;

    public void startAcceptingIncomingReplicationLinkConnections(final Topics topics) {
        this.topics = topics;
        Thread.ofVirtual().start(() -> {
            logger.info("Opening cluster entry on port {}", clusterEntryLocalPort);
            try {
                replicationLinkServerSocket = new ServerSocket(clusterEntryLocalPort);
                while (true) {
                    try {
                        var socketWithOtherNode = replicationLinkServerSocket.accept();
                        logger.info("Another node connected to this one");
                        this.otherNodes.add(socketWithOtherNode);
                        Thread.ofVirtual().start(() -> {
                            try {
                                handleIncomingMessages(socketWithOtherNode);
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
                new PrintStream(node.getOutputStream()).println(line);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void establishLink(final InetSocketAddress clusterEntryAddress) {
        logger.info("Joining cluster via {}", clusterEntryAddress);
        Thread.ofVirtual().start(() -> {
            try (var socketToOtherNode = new Socket();) {
                socketToOtherNode.connect(clusterEntryAddress);
                this.otherNodes.add(socketToOtherNode);
                handleIncomingMessages(socketToOtherNode);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void handleIncomingMessages(final Socket socketToOtherNode) throws IOException {
        var bufferedReader = new BufferedReader(new InputStreamReader(socketToOtherNode.getInputStream()));
        while (true) {
            logger.info("Listening for incoming messages from other node: {}", socketToOtherNode);
            String line = bufferedReader.readLine();
            logger.info("Got message from other broker: [{}]", line);
            var parts = line.split(",");
            switch (parts[0]) {
            case "DELIVERED":
                topics.deleteMessage(parts[2], parts[1], parts[3]);
                break;
            case "REPLICATED_MESSAGE": // REPLICATED_MESSAGE,<consumer group name>,<topic name>,<message>
                topics.createConsumerGroupForTopic(parts[2], parts[1]);
                topics.storeInConsumerGroup(parts[1], "MESSAGE," + parts[2] + "," + parts[3]);
                break;
            case "REPLICATED_SUBSCRIPTION_REQUEST":
                topics.subscribeConsumerGroupToTopic(parts[1], parts[2], false);
                break;
            default:
                logger.warn("", new IllegalArgumentException("Unknown message type in message: " + line));
            }
        }
    }
}
