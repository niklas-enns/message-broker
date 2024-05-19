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
                    var secondPart = parts[1];
                    switch (messageType) {
                    case "SUB_REQ":
                        subscriptions.add(secondPart, clientName, socketWithClient);
                        new PrintStream(socketWithClient.getOutputStream(), true).println("SUB_RESP_OK,"+secondPart);
                        break;
                    case "MESSAGE":
                        var payload = parts[2];
                        publish(secondPart, payload);
                        break;
                    case "HI_MY_NAME_IS":
                        clientName = secondPart;
                        break;
                    default:
                        logger.info("Unsupported message: {}", line);
                    }
                }
                logger.info("Stopping shoveling, because socket is closed: {}", socketWithClient.isClosed());
            } catch (IOException | EndOfStreamException e) {
                subscriptions.removeSocket(socketWithClient);
                try {
                    socketWithClient.close();
                } catch (IOException ex) {
                }
                logger.info("Handler for {} stopped, because", clientName, e);
            }
        });

    }

    private void publish(final String topic, final String message) {
        subscriptions.sendToTopic(topic, message);
    }
}
