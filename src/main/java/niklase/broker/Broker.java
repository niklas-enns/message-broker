package niklase.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Broker {
    private Subscriptions subscriptions = new Subscriptions();

    public void run(final int port) throws IOException {
        System.out.println("Starting Message Broker ...");
        var serverSocketForClients = new ServerSocket(port);

        while (true) {
            try {
                var accept = serverSocketForClients.accept();
                startNewHandler(accept);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void startNewHandler(final Socket socketWithClient) {
        Thread.ofVirtual().start(() -> {
            try {
                var bufferedReaderFromClient =
                        new BufferedReader(new InputStreamReader(socketWithClient.getInputStream()));
                while (true) {
                    var line = bufferedReaderFromClient.readLine();
                    System.out.println("<<< broker got " + line);
                    var parts = line.split(",");
                    var messageType = parts[0];
                    var topic = parts[1];
                    switch (messageType) {
                    case "SUB_REQ":
                        subscriptions.add(topic, socketWithClient);
                        break;
                    case "MESSAGE":
                        var payload = parts[2];
                        publish(topic, payload);
                        break;
                    default:
                        System.out.println("Unsupported message: " + line);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }

    private void publish(final String topic, final String line) {
        subscriptions.byTopic(topic)
                .forEach(subscription -> {
                    try {
                        new PrintStream(subscription.getOutputStream(), true).println(line);
                        System.out.println(
                                "<<< >>> broker forwarded " + line + " to " + subscription.getPort());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
