import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import niklase.broker.Broker;
import niklase.client.Client;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReplicationTest {
    public static final int CLIENT_PORT_BROKER_1 = 1500;
    public static final int REPLICATION_PORT_BROKER_1 = 1501;
    public static final int CLIENT_PORT_BROKER_2 = 1600;
    public static final int REPLICATION_PORT_BROKER_2 = 1601;
    private Broker broker1;
    private Broker broker2;
    private Client client1;
    private Client client2;

    @BeforeEach
    void beforeEach() throws IOException, InterruptedException {
        broker1 = new Broker();
        broker1.setRemoteReplicationProviderAddresses(
                List.of(new InetSocketAddress("localhost", REPLICATION_PORT_BROKER_2)));
        broker1.setLocalReplicationProviderPort(REPLICATION_PORT_BROKER_1);
        broker1.setMessageDeliveryFilter(0);
        Thread.ofVirtual().start(() -> {
            try {
                broker1.run(CLIENT_PORT_BROKER_1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        broker2 = new Broker();
        broker2.setRemoteReplicationProviderAddresses(
                List.of(new InetSocketAddress("localhost", REPLICATION_PORT_BROKER_1)));
        broker2.setLocalReplicationProviderPort(REPLICATION_PORT_BROKER_2);
        broker2.setMessageDeliveryFilter(1);
        Thread.ofVirtual().start(() -> {
            try {
                broker2.run(CLIENT_PORT_BROKER_2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(1000); // wait for exchange of replication requests
        client1 = new Client(CLIENT_PORT_BROKER_1, "C1");
        client2 = new Client(CLIENT_PORT_BROKER_2, "C2");
    }

    @AfterEach
    void tearDown() throws IOException {
        broker1.stop();
        broker2.stop();
    }

    @Test
    @DisplayName("Client receives a message via replication")
    void test0() throws IOException, InterruptedException {
        client2.subscribe("topic1");
        Thread.sleep(100);
        client1.publish("topic1", "My string data1");

        Thread.sleep(1000);
        assertEquals("My string data1", client2.getConsumedMessages("topic1").get(0));
    }

    @Test
    @DisplayName("Nodes distribute disjoint message sets although all messages are replicated")
    void test1() throws IOException, InterruptedException {
        client1.subscribe("topic1");
        client2.subscribe("topic1");
        Thread.sleep(100);
        for (int i = 0; i < 10; i++) {
            // for some reason, the hashcodes %2 for these generated messages are evenly distributed
            client1.publish("topic1", "My string data " + i);
        }
        Thread.sleep(1000);

        assertEquals(5, client1.getConsumedMessages("topic1").size());
        assertEquals(5, client2.getConsumedMessages("topic1").size());
        assertTrue(Collections.disjoint(
                client1.getConsumedMessages("topic1"),
                client2.getConsumedMessages("topic1"))
        );

        //        replicated messages which should be processed by the other node
        //        assertEquals(5,broker1.getTotalMessageCount());
        //        assertEquals(5,broker2.getTotalMessageCount());
    }

    @Test
    @DisplayName("Replicated messages are deleted after delivery")
    void test2() throws IOException, InterruptedException {
        client2.subscribe("topic1");
        Thread.sleep(100);
        client1.publish("topic1", "My string data1");

        Thread.sleep(100);
        assertEquals("My string data1", client2.getConsumedMessages("topic1").get(0));

        //both brokers are empty
        assertEquals(0, broker1.getTotalMessageCount());
        assertEquals(0, broker2.getTotalMessageCount());
    }

}