import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;

import niklase.broker.Broker;
import niklase.client.Client;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReplicationTest {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationTest.class);

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
        broker1 = new Broker("N1");
        broker1.setClusterEntryLocalPort(REPLICATION_PORT_BROKER_1);
        Thread.ofVirtual().start(() -> {
            try {
                broker1.run(CLIENT_PORT_BROKER_1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(100);
        broker2 = new Broker("N2");
        broker2.joinCluster(new InetSocketAddress("localhost", REPLICATION_PORT_BROKER_1));
        broker2.setClusterEntryLocalPort(REPLICATION_PORT_BROKER_2);
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
        broker1.setMessageDeliveryFilter(0);
        broker2.setMessageDeliveryFilter(1);
        Thread.sleep(100);
        client1.publish("topic1", "My string data1");

        Thread.sleep(1000);
        assertEquals("My string data1", client2.getConsumedMessages("topic1").get(0));
    }

    @Test
    @DisplayName("Nodes distribute disjoint message sets although all messages are replicated")
    void test1() throws IOException, InterruptedException {
        client1.subscribe("topic1", "cg1");
        Thread.sleep(100); // TODO implement concurrent REORG_DOL sessions
        client2.subscribe("topic1", "cg1");
        Thread.sleep(100);
        for (int i = 0; i < 10; i++) {
            // for some reason, the hashcodes %2 for these generated messages are evenly distributed
            client1.publish("topic1", "My string data " + i);
        }

        Thread.sleep(1000);

        printBrokerState(broker1);
        printBrokerState(broker2);


        assertEquals(5, client1.getConsumedMessages("topic1").size());
        assertEquals(5, client2.getConsumedMessages("topic1").size());
        assertTrue(Collections.disjoint(
                client1.getConsumedMessages("topic1"),
                client2.getConsumedMessages("topic1"))
        );
    }

    private void printBrokerState(final Broker broker1) {
        System.out.println(broker1.getNodeId() + ":");
        System.out.println(broker1.getTotalMessageCount() + " messages left");
        System.out.println(broker1.getIncomingMessageCount() + " incoming messages");
    }

    @Test
    @DisplayName("Replicated messages are deleted after delivery")
    void test2() throws IOException, InterruptedException {
        client2.subscribe("topic1", "cg1");
        Thread.sleep(100);
        broker1.setMessageDeliveryFilter(0);
        broker2.setMessageDeliveryFilter(1);
        Thread.sleep(100);
        client1.publish("topic1", "My string data1");

        Thread.sleep(100);
        assertEquals("My string data1", client2.getConsumedMessages("topic1").get(0));

        //both brokers are empty
        assertEquals(0, broker1.getTotalMessageCount()); //TODO implement deletion propagation
        assertEquals(0, broker2.getTotalMessageCount());
    }

    @Test
    @DisplayName("Accepting node stores message even though the topic is locally unknown")
    void test3() throws IOException, InterruptedException {
        client2.subscribe("topic1");
        client2.closeSocket();
        Thread.sleep(100);
        client1.publish("topic1", "My string data1");
        client1.publish("topic1", "My string data1");

        Thread.sleep(1000);
        assertEquals(2, broker1.getTotalMessageCount());
        assertEquals(2, broker2.getTotalMessageCount());
    }

    @Test
    @DisplayName(
            "N1 receives messages for CG1 and CG2 of topic T1 that should be distributed by N2. Currently no subscriptions for that CGs, therefore messages are only stored and not distributed. "
                    + "Then, C1 and C4 appear, subscribe to CG1 and CG2. Messages get consumed on N1")
    void test4() throws IOException, InterruptedException {
        // prepare N2 as replication-message producer
        client2.subscribe("t1", "cg1");
        client2.subscribe("t1", "cg2");
        client2.closeSocket();
        var client3 = new Client(CLIENT_PORT_BROKER_2, "C3");
        client3.publish("t1", "My string data1");

        // assert message has been replicated to broker 2
        Thread.sleep(100);
        assertEquals(2, broker2.getTotalMessageCount()); // message is stored twice. one for each cg
        assertEquals(2, broker1.getTotalMessageCount());

        var client4 = new Client(CLIENT_PORT_BROKER_1, "C4");
        client1.subscribe("t1", "cg1");
        client4.subscribe("t1", "cg2");

        Thread.sleep(100);
        broker1.setMessageDeliveryFilter(0);
        broker2.setMessageDeliveryFilter(1);
        Thread.sleep(100);

        assertEquals(1,client1.getConsumedMessages("t1").size());
        assertEquals(1,client4.getConsumedMessages("t1").size());

        Thread.sleep(100);
        assertEquals(0, broker2.getTotalMessageCount());
        assertEquals(0, broker1.getTotalMessageCount());

    }

}