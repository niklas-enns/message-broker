package niklase.broker;

import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Subscriptions {
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

    public void removeAllFromSocket(final Socket socketWithClient) {
        subscriptions.forEach((topic, sockets) -> {
            var removed = sockets.remove(socketWithClient);
            if (removed) {
                System.out.println("Unsubscribed client "+ socketWithClient.getPort() + " from " + topic);
            }
        });
    }
}
