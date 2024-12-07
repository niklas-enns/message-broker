package niklase;

import java.io.IOException;

import niklase.broker.Broker;

public class Main {
    public static void main(String[] args) throws IOException {
        new Broker("Message-Broker").run(1666);
    }
}
