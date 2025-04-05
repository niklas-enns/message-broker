package niklase.client;

import java.io.IOException;

public class ClientRunner {
    public static void main(String[] args) throws IOException, InterruptedException {
        var client = new Client(1666, "C1");
        client.subscribe("t2", "g1");
        Thread.sleep(1000);
    }
}
