import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import niklase.broker.Broker;
import niklase.client.Client;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(200);

        client1 = new Client(PORT, "C1");
        client2 = new Client(PORT, "C2");
    }

    @AfterEach
    void tearDown() throws IOException {
        broker.stop();
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
        assertEquals(1, client2.getConsumedMessages("topic1").size());
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

        client1.publish("topic1", "Hello from C1");
        client2.publish("topic1", "Hello from C2");

        Thread.sleep(100);
        broker.hook();

        assertEquals(2, client1.getConsumedMessages("topic1").size());
        assertTrue(client1.getConsumedMessages("topic1").contains("Hello from C1"));
        assertTrue(client1.getConsumedMessages("topic1").contains("Hello from C2"));

        assertEquals(2, client2.getConsumedMessages("topic1").size());
        assertTrue(client2.getConsumedMessages("topic1").contains("Hello from C1"));
        assertTrue(client2.getConsumedMessages("topic1").contains("Hello from C2"));

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

        assertEquals(1, client2.getConsumedMessages("topic1").size());
        assertEquals("My string data", client2.getConsumedMessages("topic1").get(0));

        assertEquals(1, client3.getConsumedMessages("topic1").size());
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

        assertEquals(2, client1.getConsumedMessages("topic1").size());
        assertTrue(client1.getConsumedMessages("topic1").contains("Hello from C1"));
        assertTrue(client1.getConsumedMessages("topic1").contains("Hello from C2"));

        assertEquals(2, client2.getConsumedMessages("topic1").size());
        assertTrue(client2.getConsumedMessages("topic1").contains("Hello from C2"));
        assertTrue(client2.getConsumedMessages("topic1").contains("Hello from C1"));

        assertEquals(2, client3.getConsumedMessages("topic1").size());
        assertTrue(client3.getConsumedMessages("topic1").contains("Hello from C1"));
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

        assertEquals(1, client2.getConsumedMessages("topic1").size());
        assertTrue(client2.getConsumedMessages("topic1").contains("messag"));
        assertTrue(client2.getConsumedMessages("topic2").isEmpty());
    }

    @Test
    @DisplayName("2 clients, 1 message, temporary disconnected")
    void test10() throws IOException, InterruptedException {
        client2.subscribe("topic1");
        client2.closeSocket();
        Thread.sleep(100);
        client1.publish("topic1", "My string data 1");
        client1.publish("topic1", "My string data 2");
        client1.publish("topic1", "My string data 3");

        client2.connect(PORT);

        Thread.sleep(100);
        assertEquals(3, client2.getConsumedMessages("topic1").size());
        assertTrue(client2.getConsumedMessages("topic1").contains("My string data 1"));
        assertTrue(client2.getConsumedMessages("topic1").contains("My string data 2"));
        assertTrue(client2.getConsumedMessages("topic1").contains("My string data 3"));
    }

    @Test
    @DisplayName("Load-Balancing in consumer groups")
    void test11() throws IOException, InterruptedException {
        client2.subscribe("topic1", "group1");
        client1.subscribe("topic1", "group1");
        Thread.sleep(100);
        for (int i = 0; i < 20; i++) {
            client1.publish("topic1", "My string data");
        }
        Thread.sleep(100);
        assertEquals(10,client1.getConsumedMessages("topic1").size());
        assertEquals(10,client2.getConsumedMessages("topic1").size());
    }

}