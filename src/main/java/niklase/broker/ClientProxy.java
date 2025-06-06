package niklase.broker;

import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientProxy {
    private static final Logger logger = LoggerFactory.getLogger(ClientProxy.class);
    private String name;
    private Socket socketToClient;

    ClientProxy(String name, Socket socketToClient) {
        this.name = name;
        this.socketToClient = socketToClient;
    }

    public void setSocketToClient(final Socket socketWithClient) {
        this.socketToClient = socketWithClient;
    }

    public String getName() {
        return this.name;
    }

    public Socket socketToClient() {
        return this.socketToClient;
    }

    public synchronized void clearSocketToClient(final Socket socket) {
        if (socket.equals(this.socketToClient)) {
            this.socketToClient = null;
        }
    }
}
