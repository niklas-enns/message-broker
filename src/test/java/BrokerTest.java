import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import niklase.broker.Broker;
import niklase.client.Client;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BrokerTest {
    public static final int PORT = 1888;
    private Broker broker;
    private Client client1;
    private Client client2;

    @BeforeEach
    void beforeEach() throws IOException, InterruptedException {
        broker = new Broker();
        Thread.ofVirtual().start(() -> {
            try {
                broker.run(PORT);
            } catch (Exception e) {
                System.out.println("FATAL, BROKER DIED!");
                e.printStackTrace();
            }
        });
        Thread.sleep(200);

        client1 = new Client(PORT, "C1");
        client2 = new Client(PORT, "C2");
    }

    @Test
    @DisplayName("2 clients, 1 message but no subscription")
    void test0() throws IOException, InterruptedException {
        client1.publish("topic1", "My string data");

        Thread.sleep(100);
        assertTrue(client2.getConsumedMessages("x").isEmpty());
    }

    @Test
    @DisplayName("2 clients, 1 message")
    void test() throws IOException, InterruptedException {
        client2.subscribe("topic1");
        Thread.sleep(100);
        client1.publish("topic1", "My string data");

        Thread.sleep(100);
        assertEquals("My string data", client2.getConsumedMessages("topic1").get(0));
    }

    @Test
    @DisplayName("2 clients, 1 message but receiver disappears")
    void testDisappearingClient() throws IOException, InterruptedException {
        client2.subscribe("topic1");
        client2.closeSocket();
        Thread.sleep(100);
        client1.publish("topic1", "My string data");
    }

    @Test
    @DisplayName("2 clients, 2 messages")
    void test2() throws IOException, InterruptedException {
        client1.subscribe("topic1");
        client2.subscribe("topic1");
        Thread.sleep(100);

        client1.publish("topic1", "Hello from C1");
        client2.publish("topic1", "Hello from C2");

        Thread.sleep(100);
        assertTrue(client1.getConsumedMessages("topic1").contains("Hello from C2"));
        assertTrue(client2.getConsumedMessages("topic1").contains("Hello from C1"));
    }

    @Test
    @DisplayName("3 Clients, 1 message")
    void testWithTwoProducers() throws IOException, InterruptedException {
        var client3 = new Client(PORT, "C3");

        client2.subscribe("topic1");
        client3.subscribe("topic1");
        Thread.sleep(100);

        client1.publish("topic1", "My string data");
        Thread.sleep(100);

        assertEquals("My string data", client2.getConsumedMessages("topic1").get(0));
        assertEquals("My string data", client3.getConsumedMessages("topic1").get(0));
    }

    @Test
    @DisplayName("3 Clients, 2 messages from two different clients")
    void test3() throws IOException, InterruptedException {
        var client3 = new Client(PORT, "C3");
        client1.subscribe("topic1");
        client2.subscribe("topic1");
        client3.subscribe("topic1");
        Thread.sleep(100);

        client1.publish("topic1", "Hello from C1");
        client2.publish("topic1", "Hello from C2");

        Thread.sleep(10);

        assertTrue(client2.getConsumedMessages("topic1").contains("Hello from C1"));
        assertTrue(client3.getConsumedMessages("topic1").contains("Hello from C1"));

        assertTrue(client1.getConsumedMessages("topic1").contains("Hello from C2"));
        assertTrue(client3.getConsumedMessages("topic1").contains("Hello from C2"));
    }

    @Test
    @DisplayName("Multiple topics")
    void multipleTopics() throws IOException, InterruptedException {
        client2.subscribe("topic1");
        Thread.sleep(10);
        client1.publish("topic1", "messag");
        client1.publish("topic2", "messagee"); //client2 is not subscribed

        Thread.sleep(10);
        assertTrue(client1.getConsumedMessages("topic1").isEmpty());
        assertTrue(client2.getConsumedMessages("topic1").contains("messag"));
        assertTrue(client2.getConsumedMessages("topic2").isEmpty());
    }

    @Test
    @DisplayName("2 clients, 1 message, temporary disconnected")
    @Disabled("Not implemented yet")
    void test10() throws IOException, InterruptedException {
        client2.subscribe("topic1");
        client2.closeSocket();
        Thread.sleep(100);
        client1.publish("topic1", "My string data");
        Thread.sleep(100);
        client2.connect(PORT);

        Thread.sleep(100);
        assertEquals("My string data", client2.getConsumedMessages("topic1").get(0));
    }

}