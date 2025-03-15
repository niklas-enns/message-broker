import java.io.IOException;

import niklase.broker.Broker;

public class TestUtil {
    public static void startInNewThread(final Broker broker, final int port) {
        Thread.ofVirtual().start(() -> {
            try {
                broker.run(port);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void printBrokerState(final Broker broker1) {
        System.out.println(broker1.getNodeId() + ":");
        System.out.println(broker1.getCountOfCurrentlyStoredMessages() + " messages left");
        System.out.println(broker1.getIncomingMessageCount() + " incoming messages");
        System.out.println("Message distribution rules:  " + broker1.getMessageDistributionRules());
    }

}