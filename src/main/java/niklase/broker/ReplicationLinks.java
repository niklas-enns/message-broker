package niklase.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class ReplicationLinks {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationLinks.class);
    private int clusterEntryLocalPort;
    private ServerSocket replicationLinkServerSocket;
    private Set<Node> otherNodes = new HashSet<>();
    private Topics topics;
    private String nodeId;

    private int localDolRoll;
    private List<Integer> otherNodesRolls = new LinkedList<>();
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public ReplicationLinks() {
    }

    public void startAcceptingIncomingReplicationLinkConnections(final Topics topics) {
        this.topics = topics;
        Thread.ofVirtual().start(() -> {
            MDC.put("nodeId", this.nodeId);
            logger.info("Opening cluster entry on port {}", clusterEntryLocalPort);
            try {
                replicationLinkServerSocket = new ServerSocket(clusterEntryLocalPort);
                while (true) {
                    try {
                        var socketWithOtherNode = replicationLinkServerSocket.accept();

                        Thread.ofVirtual().start(() -> {
                            MDC.put("nodeId", this.nodeId);
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

    private String getOtherNodesAsString(final Node exception) {
        var string = "";
        for (final Node otherNode : this.otherNodes) {
            if (!otherNode.id.equals(exception.id)) {
                string += otherNode.id + "," + otherNode.getAddressForNewReplicationLinks() + ",";
            }

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
                new PrintStream(node.getSocketOfEstablishedReplicationLink().getOutputStream(), true).println(line);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void establishLink(final InetSocketAddress clusterEntryAddress) {
        logger.info("Establishing replication link to {}", clusterEntryAddress);
        Thread.ofVirtual().start(() -> {
            MDC.put("nodeId", this.nodeId);
            try (var socketToOtherNode = new Socket()) {
                socketToOtherNode.connect(clusterEntryAddress);
                new PrintStream(socketToOtherNode.getOutputStream(), true).println(
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
            logger.info("waiting for new messages from other node");
            String line = bufferedReader.readLine();
            logger.info("Got message from other broker: [{}]", line);
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

                var addresses = getOtherNodesAsString(enteringNode);
                new PrintStream(socket.getOutputStream(), true).println(
                        "WELCOME_TO_THE_HOOD," + this.nodeId + ","
                                + addresses); //WELCOME_TO_THE_HOOD,<id of welcomer>,<id of other node>,<ip of other node>:<port of other node>
                this.otherNodes.add(enteringNode);
                break;
            case "REORG_DOL":
                var consumerGroupName = parts[2];
                if (thisNodeIsADistributorFor(consumerGroupName)) {
                    if (parts[1].equals("INIT")) {
                        var otherNodesRoll = Integer.parseInt(parts[3]);
                        this.localDolRoll = new Random().nextInt();
                        this.otherNodesRolls.clear();
                        addToOtherNodesRolls(otherNodesRoll);
                        this.sendToReplicationReceivers("REORG_DOL" + "," + "RESPONSE" + "," + consumerGroupName + "," + this.localDolRoll);
                        logger.info("After receiving REORG_DOL,INIT, Now waiting 50ms for other nodes to join the group of distributors for ConsumerGroup {}",
                                consumerGroupName);
                        determineForWhichMessagesThisNodeIsResponsibleFor(consumerGroupName);
                    }
                    if (parts[1].equals("RESPONSE")) {
                        var otherNodesRoll = Integer.parseInt(parts[3]);
                        addToOtherNodesRolls(otherNodesRoll);
                    }
                } else {
                    logger.info("Ignoring REORG_DOL, because I'm not a distributor for {}", consumerGroupName);
                }
                break;
            default:
                logger.warn("", new IllegalArgumentException("Unknown message type in message: " + line));
            }
        }
    }

    private synchronized void addToOtherNodesRolls(final int otherNodesRoll) {
        this.otherNodesRolls.add(otherNodesRoll);
    }

    /**
     *
     * @param consumerGroupName
     * @return true when this node has at least one client subscription to the consumerGroup.
     * It doesn't matter whether the client is connected or not.
     */
    private boolean thisNodeIsADistributorFor(final String consumerGroupName) {
        var consumerGroup = topics.getConsumerGroupByName(consumerGroupName);
        return !consumerGroup.getClientProxies().isEmpty();
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

    public Set<String> getIdsOfAllNodesWithEstablishedReplicationLinks() {
        return this.otherNodes.stream()
                .map(n -> n.id)
                .collect(Collectors.toSet());
    }

    public void setNodeId(final String nodeId) {
        this.nodeId = nodeId;
    }

    public void becomeDistributor(final String consumerGroupName) {
        this.localDolRoll = new Random().nextInt();
        this.otherNodesRolls.clear();
        sendToReplicationReceivers("REORG_DOL," + "INIT" + "," + consumerGroupName + "," + this.localDolRoll);
        logger.info("Now waiting 50ms for other nodes to join the group of distributors for ConsumerGroup {}",
                consumerGroupName);
        determineForWhichMessagesThisNodeIsResponsibleFor(consumerGroupName);
    }

    private void determineForWhichMessagesThisNodeIsResponsibleFor(final String consumerGroupName) {
        Runnable task = () -> {
            if (!this.otherNodesRolls.isEmpty()) {
                MDC.put("nodeId", this.nodeId);
                var indexOfLocalDolRoll = indexOf(this.otherNodesRolls, this.localDolRoll);
                topics.getConsumerGroupByName(consumerGroupName).getMessageProcessingFilter().setModuloRemainder(indexOfLocalDolRoll, this.otherNodesRolls.size() + 1);
            }
        };
        executor.schedule(task, 100, TimeUnit.MILLISECONDS);
    }

    //public for unit-testability
    public static int indexOf(final List<Integer> otherNodesRolls, final int localDolRoll) {
        logger.info("Determining the index of the local roll {} within the rolls of the other nodes: {}", localDolRoll,
                otherNodesRolls);
        var allRolls = new ArrayList<>(otherNodesRolls);
        allRolls.add(localDolRoll);
        Collections.sort(allRolls);
        return allRolls.indexOf(localDolRoll);
    }
}
