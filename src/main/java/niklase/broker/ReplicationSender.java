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

    private int replicationPortBroker;
    private ServerSocket socketForIncomingReplicationRequests;
    private List<OutputStream> outputStreamToMessageReplicationReceivers = new LinkedList<>();

    public void setLocalPort(final int replicationPortBroker) {
        this.replicationPortBroker = replicationPortBroker;
    }

    public void start() {
        Thread.ofVirtual().start(() -> {
            logger.info("Opening replication request socket on port {}", replicationPortBroker);
            try {
                socketForIncomingReplicationRequests = new ServerSocket(replicationPortBroker);
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

    public void stop() throws IOException {
        this.socketForIncomingReplicationRequests.close();
    }

    public void accept(final String line) {
        this.outputStreamToMessageReplicationReceivers.forEach(outputStream -> {
            new PrintStream(outputStream).println(line);
        });
    }
}
