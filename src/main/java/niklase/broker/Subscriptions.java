package niklase.broker;

import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
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
                    logger.info("Removed socket from clientproxy {} but keeping subscription to topic {}",
                            clientProxy.name,
                            topic);
                }
            });

        });
    }

    public void sendToTopic(final String topic, final String message) {
        var envelope = "MESSAGE," + topic + "," + message;
        byTopic(topic).forEach(clientProxy -> {
            clientProxy.accept(envelope);
        });

    }

    public void addClient(final String clientName, final Socket socketWithClient) {
        var setOfAllClientProxies = new HashSet<ClientProxy>();
        subscriptions.values().forEach(setOfAllClientProxies::addAll);
        final Optional<ClientProxy> clientProxyThatMatchedName = setOfAllClientProxies.stream()
                .filter(clientProxy -> clientProxy.name.equals(clientName))
                .findAny();

        if (clientProxyThatMatchedName.isPresent()) {
            clientProxyThatMatchedName.get().setSocketToClient(socketWithClient);
        } else {
            logger.info("Tried to enable client {} but no there are no ClientProxies known", clientName);
        }
    }

    static class ClientProxy {
        private String name;
        private Socket socketToClient;

        private List<String> messages = new LinkedList<>();

        ClientProxy(String name, Socket socketToClient) {
            this.name = name;
            this.socketToClient = socketToClient;
        }

        private synchronized void accept(final String envelope) {
            messages.add(envelope);
            this.flush();
        }

        private synchronized void flush() {
            if (this.socketToClient != null) {
                try {
                    messages.removeIf(envelope -> {
                        try {
                            new PrintStream(this.socketToClient.getOutputStream(), true).println(envelope);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        logger.info("<<< >>> forwarded [{}] to [{}]", envelope, this.name);
                        return true;
                    });
                    logger.info("Flushing {} finished without Exceptions. {} message(s) are left", this.name,
                            this.messages.size());
                } catch (Exception e) {
                    try {
                        socketToClient.close();
                    } catch (Exception ex) {
                        logger.info("Unable to close socket on failed send operation");
                    } finally {
                        socketToClient = null;
                    }
                }
            } else {
                logger.info("Unable to flush {} messages to [{}], because there is no client connected",
                        messages.size(), this.name);
            }

        }

        public void setSocketToClient(final Socket socketWithClient) {
            this.socketToClient = socketWithClient;
            this.flush();
        }
    }
}
