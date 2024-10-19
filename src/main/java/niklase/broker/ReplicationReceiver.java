package niklase.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationReceiver {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationReceiver.class);
    private final Topics topics;
    private List<InetSocketAddress> replicationAddresses = new LinkedList<>();

    public ReplicationReceiver(final Topics topics) {
        this.topics = topics;
    }

    public void sendReplicationRequests() {
        logger.info("Sending replication requests to {}", this.replicationAddresses);
        this.replicationAddresses.forEach(address -> {
            Thread.ofVirtual().start(() -> {
                try (var socketToOtherNode = new Socket();) {
                    socketToOtherNode.connect(address);
                    var bufferedReader = new BufferedReader(new InputStreamReader(socketToOtherNode.getInputStream()));
                    while (true) {
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
                        default:
                            logger.warn("", new IllegalArgumentException("Unknown message type in message: " + line));
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        });
    }

    public void setReplicationSenders(final List<InetSocketAddress> replicationAddresses) {
        this.replicationAddresses = replicationAddresses;
    }
}
