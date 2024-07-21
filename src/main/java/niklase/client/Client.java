package niklase.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

import niklase.broker.EndOfStreamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
    private final String name;
    private Logger logger;
    private Socket socketToBroker;
    private final MessageStore consumedMessages = new MessageStore();
    private final List<String> subscribedTopics = new LinkedList<>();

    public Client(final int port, String name) throws IOException {
        this.name = name;
        this.logger = LoggerFactory.getLogger("Client " + this.name);
        connect(port);
    }

    public void connect(final int port) throws IOException {
        logger.info("connecting to port {}", port);
        this.socketToBroker = new Socket();
        this.socketToBroker.connect(new InetSocketAddress("localhost", port), 10 * 1000);
        logger.info("connected via socket {}", this.socketToBroker.getLocalPort());
        send("HI_MY_NAME_IS," + this.name);
        shovel();
    }

    void shovel() throws IOException {
        var bufferedReader = new BufferedReader(new InputStreamReader(socketToBroker.getInputStream()));
        Thread.ofVirtual().start(() -> {
            logger.info("Start shoveling...");
            while (!socketToBroker.isClosed()) {
                try {
                    String line = bufferedReader.readLine();
                    if (line == null) {
                        throw new EndOfStreamException("End of stream from server ");
                    }
                    logger.info("<<< RAW {}", line);
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
                } catch (IOException | EndOfStreamException e) {
                    logger.info("Shoveling stopped, because", e);
                    break;
                }
            }
            logger.info("Stopping shoveling, because socket is closed");
        });
    }

    public void publish(final String topicName, final String payload) throws IOException {
        var encodedMessage = encode(topicName, payload);
        send(encodedMessage);
        logger.info(">>> Sent {} to broker", encodedMessage);
    }

    private String encode(final String topicName, final String payload) {
        return "MESSAGE," + topicName + "," + payload;
    }

    public List<String> getConsumedMessages(final String topic) {
        return this.consumedMessages.get(topic);
    }

    public void subscribe(final String topic) throws IOException, InterruptedException {
        send("SUB_REQ," + topic);
        logger.info("Sent SUB_REQ to broker, waiting for OK");
        while (!subscribedTopics.contains(topic)) {
            Thread.sleep(10);
        }
        logger.info("Got SUB_RESP_OK from broker");
    }

    public void subscribe(final String topic, final String group) throws IOException, InterruptedException {
        send("SUB_REQ," + topic + "," + group);
        logger.info("Sent SUB_REQ to broker, waiting for OK");
        while (!subscribedTopics.contains(topic)) {
            Thread.sleep(10);
        }
        logger.info("Got SUB_RESP_OK from broker");
    }

    private void send(final String text) throws IOException {
        new PrintStream(this.socketToBroker.getOutputStream(), true).println(text);
    }

    public void closeSocket() throws IOException {
        logger.info("Closing Socket {}", this.socketToBroker.getLocalPort());
        this.socketToBroker.close();
    }

    public void hook() {
        // debugging
    }
}
