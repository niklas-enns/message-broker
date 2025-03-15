import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;

import niklase.broker.Broker;
import niklase.client.Client;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReorgOfDivisionOfLabourTest {

    @Test
    @DisplayName("DoL is reorganized when the number of distributor nodes of a CG changes")
    void test0() throws IOException, InterruptedException {
        // One distributor node for CG
        final int REPLICATION_PORT_BROKER_1 = 1501;
        var broker1 = new Broker("N1");
        broker1.setClusterEntryLocalPort(REPLICATION_PORT_BROKER_1);
        TestUtil.startInNewThread(broker1, 1500);
        broker1.waitForBrokerToAcceptConnections();
        var client1 = new Client(1500, "C1");
        client1.subscribe("topic1", "cg1");

        client1.publish("topic1", "My string data 1");
        client1.publish("topic1", "My string data 2");
        client1.publish("topic1", "My string data 3");

        Thread.sleep(100); // message distribution

        assertEquals("My string data 1", client1.getConsumedMessages("topic1").get(0));
        assertEquals("My string data 2", client1.getConsumedMessages("topic1").get(1));
        assertEquals("My string data 3", client1.getConsumedMessages("topic1").get(2));

        assertTrue(broker1.isEmpty());

        // Add a second distributor node for CG
        var broker2 = new Broker("N2");
        broker2.joinCluster(new InetSocketAddress("localhost", REPLICATION_PORT_BROKER_1));
        broker2.setClusterEntryLocalPort(1601);
        TestUtil.startInNewThread(broker2, 1600);
        Thread.sleep(100); // establish replication links???

        var client2 = new Client(1600, "C2");
        client2.subscribe("topic1", "cg1");
        broker2.waitForTerminationOfFirstReorgDolSession();

        // configure DOL explicitly to be able to make assertions on the delivery
        broker1.setMessageDeliveryFilter(1);
        broker2.setMessageDeliveryFilter(0);
        client1.deleteConsumedMessages();

        client1.publish("topic1", "My string data 1");
        client1.publish("topic1", "My string data 2");
        client1.publish("topic1", "My string data 3");

        Thread.sleep(100); // message distribution

        assertEquals("My string data 1", client1.getConsumedMessages("topic1").getFirst());
        assertEquals("My string data 2", client2.getConsumedMessages("topic1").getFirst());
        assertEquals("My string data 3", client1.getConsumedMessages("topic1").get(1));

        client1.deleteConsumedMessages();

        // add a third distributor node for CG
        var broker3 = new Broker("N3");
        broker3.joinCluster(new InetSocketAddress("localhost", REPLICATION_PORT_BROKER_1));
        broker3.setClusterEntryLocalPort(1701);
        TestUtil.startInNewThread(broker3, 1700);

        Thread.sleep(100);

        // Assert established replication links
        assertTrue(broker1.getIdsOfAllNodesWithEstablishedReplicationLinks().contains("N2"));
        assertTrue(broker1.getIdsOfAllNodesWithEstablishedReplicationLinks().contains("N3"));

        assertTrue(broker2.getIdsOfAllNodesWithEstablishedReplicationLinks().contains("N3"));
        assertTrue(broker2.getIdsOfAllNodesWithEstablishedReplicationLinks().contains("N1"));

        assertTrue(broker3.getIdsOfAllNodesWithEstablishedReplicationLinks().contains("N1"));
        assertTrue(broker3.getIdsOfAllNodesWithEstablishedReplicationLinks().contains("N2"));

        // Assert negotiated division of labour
        assertNotEquals(broker1.getMessageDistributionRule(), broker2.getMessageDistributionRule());
        assertNotEquals(broker2.getMessageDistributionRule(), broker3.getMessageDistributionRule());
        assertNotEquals(broker3.getMessageDistributionRule(), broker1.getMessageDistributionRule());

        broker1.stop();
        broker2.stop();
        broker3.stop();
    }

}