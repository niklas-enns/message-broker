package niklase.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Broker {
    private static final Logger logger = LoggerFactory.getLogger(Broker.class);

    private Topics topics = new Topics();
    private ServerSocket serverSocketForClients = null;

    public void run(final int port) throws IOException {
        logger.info("Starting Message Broker on port {}", port);
        this.serverSocketForClients = new ServerSocket(port);
        while (true) {
            try {
                var accept = serverSocketForClients.accept();
                startNewHandler(accept);
            } catch (SocketException e) {
                logger.info("Stopping broker due to a SocketException...Have we been stopped regularly?");
                break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void startNewHandler(final Socket socketWithClient) {
        Thread.ofVirtual().start(() -> {
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
                        topics.subscribeConsumerGroupToTopic(topic, consumerGroupName);
                        topics.getConsumerGroupByName(consumerGroupName)
                                .add(new ClientProxy(clientName, socketWithClient));
                        new PrintStream(socketWithClient.getOutputStream(), true).println("SUB_RESP_OK," + topic);
                        break;
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
                final String finalClientName = clientName;
                getConsumerGroups().forEach(consumerGroup -> consumerGroup.clearSocket(finalClientName, socketWithClient));
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
    }

    public void hook() {
        //for debugging purposes
    }
}
