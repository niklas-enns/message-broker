package niklase.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class Broker {
    private static final Logger logger = LoggerFactory.getLogger(Broker.class);
    private String nodeId;

    private ServerSocket serverSocketForClients = null;

    private ReplicationLinks replicationLinks = new ReplicationLinks();
    private MessageProcessingFilter messageProcessingFilter = new MessageProcessingFilter();
    private Topics topics = new Topics(replicationLinks,
            new ConsumerGroupFactory(messageProcessingFilter, replicationLinks));

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
                        //TODO is it better to have another access to clientsProxies?
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
                                .add(new ClientProxy(clientName, socketWithClient));
                        new PrintStream(socketWithClient.getOutputStream(), true).println(
                                "SUB_RESP_OK," + topic + "," + consumerGroupName);
                        topics.flush(consumerGroupName); // if the topic already contains messages
                        break;
                    case "UNSUB_REQ":
                        final String finalClientName1 = clientName;
                        topics.getConsumerGroupsSubscribedTo(parts[1])
                                .forEach(consumerGroup -> consumerGroup.removeClientProxy(finalClientName1));
                        topics.tidy(parts[1]);
                        new PrintStream(socketWithClient.getOutputStream(), true).println("UNSUB_RESP_OK," + topic);
                        continue;
                    case "MESSAGE":
                        var payload = parts[2];
                        topics.accept(topic, payload);
                        break;
                    default:
                        logger.info("Unsupported message: {}", line);
                    }
                }
                logger.info("Stopping shoveling, because socket is closed: {}", socketWithClient.isClosed());
            } catch (IOException | EndOfStreamException e) {
                getConsumerGroups().forEach(
                        consumerGroup -> consumerGroup.clearSocket(socketWithClient));
                try {
                    socketWithClient.close();
                } catch (IOException ex) {
                    logger.info("Unable to close socketWithClient", e);
                }
                logger.info("Handler for {} stopped, because", clientName, e);
            }
        });

    }

    private Set<ConsumerGroup> getConsumerGroups() {
        return topics.getAllConsumerGroups();
    }

    public void stop() throws IOException {
        logger.info("Stopping broker....");
        this.serverSocketForClients.close();
        this.replicationLinks.stop();
    }

    public void hook() {
        //for debugging purposes
    }

    public void joinCluster(final InetSocketAddress clusterEntryAddress) {
        this.replicationLinks.establishLink(clusterEntryAddress);
    }

    public void setClusterEntryLocalPort(final int clusterEntryLocalPort) {
        this.replicationLinks.setClusterEntryLocalPort(clusterEntryLocalPort);
    }

    public void setMessageDeliveryFilter(final int moduloRemainder) {
        this.messageProcessingFilter.setModuloRemainder(moduloRemainder);
    }

    public long getTotalMessageCount() {
        return this.getConsumerGroups().stream().mapToLong(ConsumerGroup::getTotalMessageCount).sum();
    }

    public Set<String> getIdsOfAllnodesWithEstablishedReplicationLinks() {
        return replicationLinks.getIdsOfAllnodesWithEstablishedReplicationLinks();
    }
}
