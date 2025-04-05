package niklase;

import java.io.IOException;
import java.net.InetSocketAddress;

import niklase.broker.Broker;

public class MainWithCluster {
    public static void main(String[] args) throws IOException {
        var broker = new Broker("Message-Broker-Node-2");
        broker.joinCluster(new InetSocketAddress("localhost", 1777));
        broker.run(2666);
    }
}
