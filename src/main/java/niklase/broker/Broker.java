package niklase.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class Broker {
    private static final Logger logger = LoggerFactory.getLogger(Broker.class);
    private String nodeId;

    private ServerSocket serverSocketForClients = null;

    private ReplicationLinks replicationLinks = new ReplicationLinks();
    private Topics topics = new Topics(replicationLinks,
            new ConsumerGroupFactory(replicationLinks));
    private int incomingMessageCounter;

    public Broker(final String nodeId) {
        this.nodeId = nodeId;
        replicationLinks.setNodeId(nodeId);
    }

    public void run(final int port) throws IOException {
        MDC.put("nodeId", this.nodeId);
        logger.info("Starting Message Broker");
        replicationLinks.startAcceptingIncomingReplicationLinkConnections(topics);
        logger.info("Opening socket for clients on port {}", port);
        this.serverSocketForClients = new ServerSocket(port);
        while (true) {
            try {
                var accept = serverSocketForClients.accept();
                startNewIncomingClientMessageHandler(accept);
            } catch (SocketException e) {
                logger.info("Stopping broker due to a SocketException...Have we been stopped regularly?");
                break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void startNewIncomingClientMessageHandler(final Socket socketWithClient) {
        Thread.ofVirtual().start(() -> {
            MDC.put("nodeId", this.nodeId);
            var clientName = "";
            try {
                var bufferedReaderFromClient =
                        new BufferedReader(new InputStreamReader(socketWithClient.getInputStream()));
                while (!socketWithClient.isClosed()) {
                    var line = bufferedReaderFromClient.readLine();
                    logger.info("<<< RAW {}", line);
                    if (line == null) {
                        throw new EndOfStreamException("End of stream from client " + clientName);
                    }
                    var parts = line.split(",");
                    if (parts.length == 1) {
                        logger.info("Unsupported message: {}", line);
                        continue;
                    }
                    var messageType = parts[0];
                    var topic = parts[1];
                    switch (messageType) {
                    case "HI_MY_NAME_IS":
                        clientName = parts[1];
                        final String finalClientName = clientName;
                        getConsumerGroups().forEach(
                                consumerGroup -> consumerGroup.setSocket(finalClientName, socketWithClient));
                        break;
                    case "SUB_REQ":
                        var consumerGroupName = UUID.randomUUID().toString();
                        if (parts.length == 3) {
                            consumerGroupName = parts[2];
                        }
                        replicationLinks.acceptSubscriptionRequest(topic, consumerGroupName);
                        topics.subscribeConsumerGroupToTopic(topic, consumerGroupName);
                        topics.getConsumerGroupByName(consumerGroupName)
                                .addConnectedClient(new ClientProxy(clientName, socketWithClient));
                        send("SUB_RESP_OK," + topic + "," + consumerGroupName + topic, socketWithClient);
                        topics.flush(consumerGroupName); // if the topic already contains messages
                        break;
                    case "UNSUB_REQ":
                        final String finalClientName1 = clientName;
                        topics.getConsumerGroupsSubscribedTo(parts[1])
                                .forEach(consumerGroup -> consumerGroup.removeClientProxy(finalClientName1));
                        topics.tidy(parts[1]);
                        send("UNSUB_RESP_OK," + topic, socketWithClient);
                        continue;
                    case "MESSAGE":
                        incomingMessageCounter++;
                        var payload = parts[2];
                        topics.accept(topic, payload);
                        break;
                    case "SUB_REQ_STATS":
                        final List<String> messages = List.of(
                                "STATS,NODE_ID," + this.nodeId,
                                "STATS,COUNT_CURRENTLY_STORED_MESSAGES," + this.getCountOfCurrentlyStoredMessages(),
                                "STATS,COUNT_RECEIVED_MESSAGES," + this.incomingMessageCounter,
                                "STATS,COUNT_DISTRIBUTED_MESSAGES," + this.getCountOfDistributedMessages(),
                                "STATS,COUNT_TOPICS," + this.topics.getAllConsumerGroups().size(),
                                "STATS,CONSUMER_GROUPS," + this.topics.getAllConsumerGroups().stream().map(
                                        ConsumerGroup::getName).toList(),
                                "STATS,MESSAGE_DISTRIBUTION_RULES," + this.getMessageDistributionRules(),
                                "STATS,ESTABLISHED_REPLICATION_LINKS," + this.getIdsOfAllNodesWithEstablishedReplicationLinks()
                        );

                        messages.forEach(m -> {
                            try {
                                send(m, socketWithClient);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });


                        break;
                    default:
                        logger.info("Unsupported message: XXX {}", line);
                    }
                }
                logger.info("Stopping shoveling, because socket with {} is closed: {}", clientName,
                        socketWithClient.isClosed());
            } catch (IOException | EndOfStreamException e) {
                getConsumerGroups().forEach(
                        consumerGroup -> consumerGroup.clearSocket(socketWithClient));
                try {
                    logger.info("Broker is closing socket to {}, because of ", clientName, e);
                    socketWithClient.close();
                } catch (IOException ex) {
                    logger.info("Unable to close socketWithClient", e);
                }
                logger.info("Handler for {} stopped, because", clientName, e);
            }
        });
    }

    private long getCountOfDistributedMessages() {
        return this.getConsumerGroups().stream().mapToLong(ConsumerGroup::getDistributedMessagesCount).sum();
    }

    private void send(final String text, final Socket socket) throws IOException {
        new PrintStream(socket.getOutputStream(), true).println(text);
    }

    private Set<ConsumerGroup> getConsumerGroups() {
        return topics.getAllConsumerGroups();
    }

    public void stop() throws IOException {
        logger.info("Stopping broker....");
        this.serverSocketForClients.close();
        this.replicationLinks.stop();
    }

    public void joinCluster(final InetSocketAddress clusterEntryAddress) {
        this.replicationLinks.establishLink(clusterEntryAddress);
    }

    public void setClusterEntryLocalPort(final int clusterEntryLocalPort) {
        this.replicationLinks.setClusterEntryLocalPort(clusterEntryLocalPort);
    }

    /**
     * For testing purposes, a way to configure DOL explicitly
     *
     * @param moduloRemainder
     * @param countOfDistributorNodes
     * @param consumerGroupName
     */
    public void setMessageDeliveryFilter(final int moduloRemainder, final int countOfDistributorNodes,
            final String consumerGroupName) {
        this.topics.getConsumerGroupByName(consumerGroupName).getMessageProcessingFilter()
                .setModuloRemainder(moduloRemainder, countOfDistributorNodes);
    }

    public long getCountOfCurrentlyStoredMessages() {
        return this.getConsumerGroups().stream().mapToLong(ConsumerGroup::getTotalMessageCount).sum();
    }

    public Set<String> getIdsOfAllNodesWithEstablishedReplicationLinks() {
        return replicationLinks.getIdsOfAllNodesWithEstablishedReplicationLinks();
    }

    /**
     * @return the count of messages that are stored locally
     */
    public int getIncomingMessageCount() {
        return this.incomingMessageCounter;
    }

    /**
     * @return true if the broker contains zero messages
     */
    public boolean isEmpty() {
        return this.getCountOfCurrentlyStoredMessages() == 0;
    }

    public String getNodeId() {
        return this.nodeId;
    }

    public String getMessageDistributionRule(final String consumerGroupName) {
        return this.topics.getConsumerGroupByName(consumerGroupName).getMessageProcessingFilter()
                .getMessageDistributionRule();
    }

    public void waitForBrokerToAcceptConnections() throws InterruptedException {
        while (this.serverSocketForClients == null) {
            Thread.sleep(1);
        }
    }

    public void waitForReplicationLinks(final int count) throws InterruptedException {
        while (this.replicationLinks.getIdsOfAllNodesWithEstablishedReplicationLinks().size() < count) {
            Thread.sleep(1);
        }
    }

    public void waitForTerminationOfFirstReorgDolSession(final String consumerGroupName) throws InterruptedException {
        while (this.getMessageDistributionRule(consumerGroupName).equals("all")) {
            System.out.println("Waiting 10ms for initial REORG_DOL session");
            Thread.sleep(10);
        }
    }

    public String getMessageDistributionRules() {
        return this.topics.getAllConsumerGroups().stream()
                .map(cg -> cg.getName() + ": " + cg.getMessageProcessingFilter().getMessageDistributionRule()).collect(
                        Collectors.joining(","));
    }
}
