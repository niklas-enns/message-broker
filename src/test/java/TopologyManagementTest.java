import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;

import niklase.broker.Broker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TopologyManagementTest {

    private Broker broker1;
    private Broker broker2;
    private Broker broker3;

    @BeforeEach
    void beforeEach() throws InterruptedException {
        final int REPLICATION_PORT_BROKER_1 = 1501;
        broker1 = new Broker("N1");
        broker1.setClusterEntryLocalPort(REPLICATION_PORT_BROKER_1);
        Thread.ofVirtual().start(() -> {
            try {
                broker1.run(1500);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(100);
        broker2 = new Broker("N2");
        broker2.joinCluster(new InetSocketAddress("localhost", REPLICATION_PORT_BROKER_1));
        broker2.setClusterEntryLocalPort(1601);
        Thread.ofVirtual().start(() -> {
            try {
                broker2.run(1600);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(100);

        broker3 = new Broker("N3");
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

    }

    @AfterEach
    void tearDown() throws IOException {
        broker1.stop();
        broker2.stop();
    }

    @Test
    @DisplayName(
            "N2 and N3 join the cluster via N1. N2 and N3 have established a replication link based on the mediation of N1")
    void test0() throws IOException {
        //assert N1 has ReplicationLinks with N2 and N3
        assertTrue(broker1.getIdsOfAllnodesWithEstablishedReplicationLinks().contains("N2"));
        assertTrue(broker1.getIdsOfAllnodesWithEstablishedReplicationLinks().contains("N3"));

        //assert N2 has ReplicationLinks with N1 and N3
        assertTrue(broker2.getIdsOfAllnodesWithEstablishedReplicationLinks().contains("N1"));
        assertTrue(broker2.getIdsOfAllnodesWithEstablishedReplicationLinks().contains("N3"));

        //assert N3 has ReplicationLinks with N1 and N2
        assertTrue(broker3.getIdsOfAllnodesWithEstablishedReplicationLinks().contains("N1"));
        assertTrue(broker3.getIdsOfAllnodesWithEstablishedReplicationLinks().contains("N2"));

        broker1.stop();
        broker2.stop();
        broker3.stop();
    }

}