package niklase.broker;

public interface DeliveryPropagator {
    public void accept(String envelope, String consumerGroup);
}
