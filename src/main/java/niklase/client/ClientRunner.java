package niklase.client;

import java.io.IOException;

public class ClientRunner {
    public static void main(String[] args) throws IOException, InterruptedException {
        var client = new Client(3666, "C1");
        //client.subscribe("t2", "g1");
        //publish20(client);
        Thread.sleep(1000);
    }

    private static void publish20(final Client client) throws IOException {
        for (int i = 0; i < Math.pow(2,10); i++) {
            client.publish("t2", "m1" + Math.random());
        }
    }
}
