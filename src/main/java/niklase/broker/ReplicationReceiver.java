package niklase.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationReceiver {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationReceiver.class);
    private List<InetSocketAddress> replicationAddresses = new LinkedList<>();
    private Consumer<String> consumer;

    public void sendReplicationRequests() {
        logger.info("Sending replication requests to {}", this.replicationAddresses);
        this.replicationAddresses.forEach(address -> {
            Thread.ofVirtual().start(() -> {
                try (var socketToOtherNode = new Socket();) {
                    socketToOtherNode.connect(address);
                    var bufferedReader = new BufferedReader(new InputStreamReader(socketToOtherNode.getInputStream()));
                    while (true) {
                        String line = bufferedReader.readLine();
                        logger.info("Got message via replication from other broker: [{}]", line);
                        consumer.accept(line);
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

    public void setMessageAcceptor(Consumer<String> consumer) {
        this.consumer = consumer;
    }
}
