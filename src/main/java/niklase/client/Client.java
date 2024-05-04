package niklase.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

public class Client {
    private final Socket socketToBroker;
    private final String name;
    private final List<String> consumedMessages = new LinkedList<>();

    public Client(final int port, String name) throws IOException {
        this.name = name;
        socketToBroker = new Socket("localhost", port);
        System.out.println("client " + this.name + " connected via socket " + socketToBroker.getLocalPort());
        shovel();
    }

    void shovel() throws IOException {
        var bufferedReader = new BufferedReader(new InputStreamReader(socketToBroker.getInputStream()));
        Thread.ofVirtual().start(() -> {
            while (true) {
                try {
                    String line = bufferedReader.readLine();
                    System.out.println("<<< client " + name + " read line: " + line);
                    this.consumedMessages.add(line);
                } catch (IOException e) {
                }
            }
        });
    }

    public void publish(final String topicName, final String payload) throws IOException {
        var encodedMessage = encode(topicName, payload);
        socketToBroker.getOutputStream()
                .write((encodedMessage + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
        System.out.println(">>> Sent " + encodedMessage + " to broker");
    }

    private String encode(final String topicName, final String payload) {
        return "MESSAGE," + topicName + "," + payload;
    }

    public List<String> getConsumedMessages() throws IOException {
        return this.consumedMessages;
    }

    public void subscribe(final String topic) throws IOException {
        socketToBroker.getOutputStream().write(("SUB_REQ,"+ topic + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
    }

    public void closeSocket() throws IOException {
        this.socketToBroker.close();
    }
}
