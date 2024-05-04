package niklase.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Broker {
    private static final Logger logger = LoggerFactory.getLogger(Broker.class);

    private Subscriptions subscriptions = new Subscriptions();

    public void run(final int port) throws IOException {
        logger.info("Starting Message Broker on port {}", port);
        try (var serverSocketForClients = new ServerSocket(port)) {
            while (true) {
                try {
                    var accept = serverSocketForClients.accept();
                    startNewHandler(accept);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void startNewHandler(final Socket socketWithClient) {
        Thread.ofVirtual().start(() -> {
            try {
                var bufferedReaderFromClient =
                        new BufferedReader(new InputStreamReader(socketWithClient.getInputStream()));
                while (!socketWithClient.isClosed()) {
                    var line = bufferedReaderFromClient.readLine();
                    logger.info("<<< RAW {}", line);
                    if (line == null) {
                        throw new EndOfStreamException("End of stream from client " + socketWithClient.getPort());
                    }
                    var parts = line.split(",");
                    if (parts.length == 1) {
                        logger.info("Unsupported message: {}", line);
                        continue;
                    }
                    var messageType = parts[0];
                    var topic = parts[1];
                    switch (messageType) {
                    case "SUB_REQ":
                        subscriptions.add(topic, socketWithClient);
                        socketWithClient.getOutputStream().write(("SUB_RESP_OK,"+topic +  System.lineSeparator()).getBytes(
                                StandardCharsets.UTF_8));
                        break;
                    case "MESSAGE":
                        var payload = parts[2];
                        publish(topic, payload);
                        break;
                    default:
                        logger.info("Unsupported message: {}", line);
                    }
                }
                logger.info("Stopping shoveling, because socket is closed: {}", socketWithClient.isClosed());
            } catch (IOException | EndOfStreamException e) {
                subscriptions.removeAllOfSocket(socketWithClient);
                try {
                    socketWithClient.close();
                } catch (IOException ex) {
                }
                logger.info("Handler for " + socketWithClient.getPort() + " stopped, because", e);
            }
        });

    }

    private void publish(final String topic, final String message) {
        subscriptions.byTopic(topic)
                .forEach(socket -> {
                    try {
                        var envelope = "MESSAGE," + topic + "," + message;
                        new PrintStream(socket.getOutputStream(), true).println(envelope);
                        logger.info("<<< >>> broker forwarded {} to {}", envelope, socket.getPort());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
