package niklase.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

public class Broker {
    private Set<Socket> socketSet = new HashSet<>();
    private Subscriptions subscriptions = new Subscriptions();

    public void run(final int port) throws IOException {
        System.out.println("niklase.broker.Broker starting...");
        var serverSocketForClients = new ServerSocket(port);

        while (true) {
            try {
                var accept = serverSocketForClients.accept();
                startNewHandler(accept);
                socketSet.add(accept);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void startNewHandler(final Socket messageSender) {
        Thread.ofVirtual().start(() -> {
            try {
                var bufferedReaderFromProducer =
                        new BufferedReader(new InputStreamReader(messageSender.getInputStream()));
                while (true) {
                    var line = bufferedReaderFromProducer.readLine();
                    System.out.println("<<< broker got " + line);
                    var parts = line.split(",");
                    var messageType = parts[0];
                    var topic = parts[1];
                    switch (messageType) {
                    case "SUB_REQ":
                        subscriptions.add(topic, messageSender);
                        //TODO respond with OK
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
                .forEach(s -> {
                    try {
                        new PrintStream(s.getOutputStream(), true).println(line);
                        System.out.println(
                                "<<< >>> broker forwarded " + line + " to " + s.getPort());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
