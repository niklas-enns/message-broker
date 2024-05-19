package niklase.broker;

import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Subscriptions {
    private static final Logger logger = LoggerFactory.getLogger(Subscriptions.class);
    private final HashMap<String, Set<ClientProxy>> subscriptions = new HashMap<>();

    void add(String topic, final String clientName, Socket socket) {
        var clientRefs = subscriptions.get(topic);
        var newClientRef = new ClientProxy(clientName, socket);
        if (clientRefs == null) {
            var refs = new HashSet<ClientProxy>();
            refs.add(newClientRef);
            subscriptions.put(topic, refs);
        } else {
            clientRefs.add(newClientRef);
        }
    }

    private Set<ClientProxy> byTopic(final String topic) {
        var clientRefs = subscriptions.get(topic);
        return clientRefs == null ? new HashSet<>() : clientRefs;
    }

    public void removeSocket(final Socket socketWithClient) {
        subscriptions.forEach((topic, clientProxies) -> {
            clientProxies.forEach(clientProxy -> {
                if (clientProxy.socketToClient == socketWithClient) {
                    clientProxy.socketToClient = null;
                    logger.info("Removed socket from clientproxy {} but keeping subscription to topic {}", clientProxy.name,
                            topic);
                }
            });

        });
    }

    public void sendToTopic(final String topic, final String message) {
        var envelope = "MESSAGE," + topic + "," + message;
        byTopic(topic).forEach(clientProxy -> {
            try {
                clientProxy.send(envelope);
            } catch (IOException e) {
                logger.error("Failed to deliver message [{}] via topic [{}] to client [{}]", message, topic, clientProxy);
            }
        });

    }

    static class ClientProxy {
        private String name;
        private Socket socketToClient;

        ClientProxy(String name, Socket socketToClient) {
            this.name = name;
            this.socketToClient = socketToClient;
        }

        public void send(final String envelope) throws IOException {
            if (this.socketToClient != null) {
                new PrintStream(this.socketToClient.getOutputStream(), true).println(envelope);
                logger.info("<<< >>> forwarded [{}] to [{}]", envelope, this.name);
            } else {
                logger.info("could not forward [{}] to [{}], because there is no client connected", envelope, this.name);
            }

        }
    }
}
