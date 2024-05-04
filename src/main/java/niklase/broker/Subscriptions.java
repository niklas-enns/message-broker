package niklase.broker;

import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Subscriptions {
    private static final Logger logger = LoggerFactory.getLogger(Subscriptions.class);
    private final HashMap<String, Set<Socket>> subscriptions = new HashMap<>();

    void add(String topic, Socket socket) {
        var sockets = subscriptions.get(topic);
        if (sockets == null) {
            var sockets1 = new HashSet<Socket>();
            sockets1.add(socket);
            subscriptions.put(topic, sockets1);
        } else {
            sockets.add(socket);
        }
    }

    public Set<Socket> byTopic(final String topic) {
        var sockets = subscriptions.get(topic);
        return sockets == null ? new HashSet<>() : sockets;
    }

    public void removeAllOfSocket(final Socket socketWithClient) {
        subscriptions.forEach((topic, sockets) -> {
            var removed = sockets.remove(socketWithClient);
            if (removed) {
                logger.info("Unsubscribed client {} from {}", socketWithClient.getPort(), topic);
            }
        });
    }
}
