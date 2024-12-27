import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;

import niklase.broker.Broker;
import niklase.client.Client;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReorgOfDivisionOfLabourTest {

    @Test
    @DisplayName("DoL is reorganized when the number of distributor nodes of a consumer group changes")
    void test0() throws IOException, InterruptedException {
        final int REPLICATION_PORT_BROKER_1 = 1501;
        var broker1 = new Broker("N1");
        broker1.setClusterEntryLocalPort(REPLICATION_PORT_BROKER_1);
        Thread.ofVirtual().start(() -> {
            try {
                broker1.run(1500);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        var client1 = new Client(1500, "C1");
        client1.subscribe("topic1", "cg1");

        var broker2 = new Broker("N2");
        broker2.joinCluster(new InetSocketAddress("localhost", REPLICATION_PORT_BROKER_1));
        broker2.setClusterEntryLocalPort(1601);
        Thread.ofVirtual().start(() -> {
            try {
                broker2.run(1600);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        System.out.println();
        System.out.println("#########################################");

        Thread.sleep(100); // join cluster
        var client2 = new Client(1600, "C2");
        client2.subscribe("topic1", "cg1");
        Thread.sleep(500); // await reorg of DoL

        client1.publish("topic1", "My string data 1");
        client1.publish("topic1", "My string data 2");
        client1.publish("topic1", "My string data 3");

        Thread.sleep(100); // message distribution

        //TODO fix flaky tests, because DOL is decided by a random number
        assertEquals("My string data 1", client1.getConsumedMessages("topic1").getFirst());
        assertEquals("My string data 2", client2.getConsumedMessages("topic1").getFirst());
        assertEquals("My string data 3", client1.getConsumedMessages("topic1").get(1));

        var broker3 = new Broker("N3");
        broker3.joinCluster(new InetSocketAddress("localhost", REPLICATION_PORT_BROKER_1));
        broker3.setClusterEntryLocalPort(1701);
        Thread.ofVirtual().start(() -> {
            try {
                broker3.run(1700);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(100);

        broker1.stop();

        broker2.stop();
        //broker3.stop();
    }

}