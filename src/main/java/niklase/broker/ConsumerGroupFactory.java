package niklase.broker;

public class ConsumerGroupFactory {
    private final ReplicationLinks replicationLinks;

    public ConsumerGroupFactory(final ReplicationLinks replicationLinks) {
        this.replicationLinks = replicationLinks;
    }

    public ConsumerGroup create(final String consumerGroupName) {
        var consumerGroup = new ConsumerGroup(consumerGroupName, new MessageProcessingFilter(), replicationLinks);
        return consumerGroup;
    }
}
