package niklase.broker;

import static org.junit.jupiter.api.Assertions.*;

import java.net.Socket;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConsumerGroupTest {
    private MessageProcessingFilter messageProcessingFilter = new MessageProcessingFilter();
    private ReplicationLinks replicationLinks = null;

    @Test
    @DisplayName("No clients")
    void getNextClientProxyWithSocket() {
        var consumerGroup = new ConsumerGroup("", messageProcessingFilter, replicationLinks);
        assertNull(consumerGroup.getNextClientProxyWithSocket());
    }

    @Test
    @DisplayName("1 client")
    void gncpws1() {
        var consumerGroup = new ConsumerGroup("", messageProcessingFilter, replicationLinks);
        var c1 = new ClientProxy("c1", new Socket());
        consumerGroup.addConnectedClient(c1);

        assertEquals(c1, consumerGroup.getNextClientProxyWithSocket());
        assertEquals(c1, consumerGroup.getNextClientProxyWithSocket());
    }

    @Test
    @DisplayName("2 client")
    void gncpw2() {
        var consumerGroup = new ConsumerGroup("", messageProcessingFilter, replicationLinks);
        var c1 = new ClientProxy("c1", new Socket());
        var c2 = new ClientProxy("c2", new Socket());

        consumerGroup.addConnectedClient(c1);
        consumerGroup.addConnectedClient(c2);

        assertEquals(c1, consumerGroup.getNextClientProxyWithSocket());
        assertEquals(c2, consumerGroup.getNextClientProxyWithSocket());
        assertEquals(c1, consumerGroup.getNextClientProxyWithSocket());
        assertEquals(c2, consumerGroup.getNextClientProxyWithSocket());
    }

    @Test
    @DisplayName("2 client, first looses socket and then the second aswell")
    void gncpw3() {
        var consumerGroup = new ConsumerGroup("", messageProcessingFilter, replicationLinks);
        var c1 = new ClientProxy("c1", new Socket());
        var c2 = new ClientProxy("c1", new Socket());

        consumerGroup.addConnectedClient(c1);
        consumerGroup.addConnectedClient(c2);

        assertEquals(c1, consumerGroup.getNextClientProxyWithSocket());
        assertEquals(c2, consumerGroup.getNextClientProxyWithSocket());

        c1.clearSocketToClient(c1.socketToClient());

        assertEquals(c2, consumerGroup.getNextClientProxyWithSocket());

        c2.clearSocketToClient(c2.socketToClient());

        assertNull(consumerGroup.getNextClientProxyWithSocket());
    }

}