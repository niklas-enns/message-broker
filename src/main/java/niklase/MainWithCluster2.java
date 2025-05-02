package niklase;

import java.io.IOException;
import java.net.InetSocketAddress;

import niklase.broker.Broker;

public class MainWithCluster2 {
    public static void main(String[] args) throws IOException {
        var broker = new Broker("Message-Broker-Node-3");
        broker.joinCluster(new InetSocketAddress("localhost", 1777));
        broker.setClusterEntryLocalPort(3777);
        broker.run(3666);
    }
}
