package niklase.broker;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationSender {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationSender.class);

    private int portForListeningForReplicationRequests;
    private ServerSocket socketForIncomingReplicationRequests;
    private List<OutputStream> outputStreamToMessageReplicationReceivers = new LinkedList<>();

    public void start() {
        Thread.ofVirtual().start(() -> {
            logger.info("Opening replication request socket on port {}", portForListeningForReplicationRequests);
            try {
                socketForIncomingReplicationRequests = new ServerSocket(portForListeningForReplicationRequests);
                while (true) {
                    try {
                        var socketWithOtherNode = socketForIncomingReplicationRequests.accept();
                        logger.info("Adding node to replication receivers");
                        this.outputStreamToMessageReplicationReceivers.add(socketWithOtherNode.getOutputStream());
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

    public void setPortForListeningForReplicationRequests(final int replicationPortBroker) {
        this.portForListeningForReplicationRequests = replicationPortBroker;
    }

    public void stop() throws IOException {
        this.socketForIncomingReplicationRequests.close();
    }

    /**
     *
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
        this.outputStreamToMessageReplicationReceivers.forEach(outputStream -> {
            new PrintStream(outputStream).println(line);
        });
    }
}
