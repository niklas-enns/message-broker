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
}