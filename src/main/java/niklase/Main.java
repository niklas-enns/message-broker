package niklase;

import java.io.IOException;

import niklase.broker.Broker;

public class Main {
    public static void main(String[] args) throws IOException {
        var broker = new Broker("Message-Broker");
        broker.setClusterEntryLocalPort(1777);
        broker.run(1666);
    }
}
