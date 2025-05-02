package niklase.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import niklase.broker.EndOfStreamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
    private final String name;
    private Logger logger;
    private Socket socketToBroker;
    private final MessageStore messageStore = new MessageStore();
    private final List<String> subscribedTopics = new LinkedList<>();
    private Consumer<String> statsCallBack = (string) -> {
    };

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
                        continue;
                    case "UNSUB_RESP_OK":
                        this.subscribedTopics.remove(parts[1]);
                        continue;
                    case "MESSAGE":
                        this.messageStore.add(parts[1], parts[2]);
                        continue;
                    case "STATS":
                        this.statsCallBack.accept(line.substring(line.indexOf(",")+1));
                        break;
                    default:
                        logger.warn("Unknown message: {}", parts);

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
        return this.messageStore.get(topic);
    }

    public void subscribe(final String topic) throws IOException, InterruptedException {
        send("SUB_REQ," + topic);
        logger.info("Sent SUB_REQ to broker, waiting for OK");
        while (!subscribedTopics.contains(topic)) {
            Thread.sleep(10);
        }
        logger.info("Got SUB_RESP_OK from broker");
    }

    /**
     * Subscribes a client to a topic via an explicit consumer group. Multiple subscriptions to the separate topic are
     * not supported and will result in undefined behaviour
     */
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

    public void unsubscribe(final String topic) throws IOException, InterruptedException {
        send("UNSUB_REQ," + topic);
        logger.info("Sent UNSUB_REQ to broker, waiting for OK");
        while (subscribedTopics.contains(topic)) {
            Thread.sleep(10);
        }
        logger.info("Got UNSUB_RESP_OK from broker");
    }

    public void deleteConsumedMessages() {
        messageStore.deleteAllMessages();
    }

    public void subscribeToBrokerStats(Consumer<String> callBack) throws IOException {
        send("SUB_REQ_STATS,TODO");
        this.statsCallBack = callBack;
    }
}
