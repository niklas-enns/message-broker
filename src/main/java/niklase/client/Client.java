package niklase.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private final Socket socketToBroker;
    private final String name;
    private final MessageStore consumedMessages = new MessageStore();
    private final List<String> subscribedTopics = new LinkedList<>();

    public Client(final int port, String name) throws IOException {
        this.name = name;
        logger.info("{} is connecting to port {}", this.name, port);
        socketToBroker = new Socket();
        socketToBroker.connect(new InetSocketAddress("localhost", port), 10*1000);
        logger.info("client {} connected via socket {}", this.name, socketToBroker.getLocalPort());
        shovel();
    }

    void shovel() throws IOException {
        var bufferedReader = new BufferedReader(new InputStreamReader(socketToBroker.getInputStream()));
        Thread.ofVirtual().start(() -> {
            while (!socketToBroker.isClosed()) {
                try {
                    String line = bufferedReader.readLine();
                    logger.info("<<< client {} read line: {}", name, line);
                    var parts = line.split(",");
                    switch (parts[0]) {
                    case "SUB_RESP_OK":
                        this.subscribedTopics.add(parts[1]);
                        this.consumedMessages.init(parts[1]);
                        continue;
                    case "MESSAGE":
                        this.consumedMessages.add(parts[1], parts[2]);
                    default:

                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public void publish(final String topicName, final String payload) throws IOException {
        var encodedMessage = encode(topicName, payload);
        socketToBroker.getOutputStream()
                .write((encodedMessage + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
        logger.info(">>> Sent {} to broker", encodedMessage);
    }

    private String encode(final String topicName, final String payload) {
        return "MESSAGE," + topicName + "," + payload;
    }

    public List<String> getConsumedMessages(final String topic) {
        return this.consumedMessages.get(topic);
    }

    public void subscribe(final String topic) throws IOException, InterruptedException {
        socketToBroker.getOutputStream().write(("SUB_REQ,"+ topic + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
        logger.info("Sent SUB_REQ to broker, waiting for OK");
        while (!subscribedTopics.contains(topic)) {
            Thread.sleep(10);
        }
    }

    public void closeSocket() throws IOException {
        this.socketToBroker.close();
    }
}
