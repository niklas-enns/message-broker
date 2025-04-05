import java.io.IOException;
import java.util.LinkedList;

import niklase.broker.Broker;
import niklase.client.Client;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StatsTest {
    public static final int PORT = 1888;
    private Broker broker;
    private Client client1;

    @BeforeEach
    void beforeEach() throws IOException, InterruptedException {
        broker = new Broker("N1");
        TestUtil.startInNewThread(broker, PORT);
        broker.waitForBrokerToAcceptConnections();

        client1 = new Client(PORT, "C1");
    }

    @AfterEach
    void tearDown() throws IOException {
        broker.stop();
    }

    @Test
    @DisplayName("initial stats")
    void test0() throws IOException, InterruptedException {
        var messages = new LinkedList();
        client1.subscribeToBrokerStats(messages::add);

        Thread.sleep(100);
        assertTrue(messages.contains("COUNT_CURRENTLY_STORED_MESSAGES,0"));
        assertTrue(messages.contains("COUNT_RECEIVED_MESSAGES,0"));
        assertTrue(messages.contains("COUNT_TOPICS,0"));

    }
}