package niklase.broker;

public class ConsumerGroupFactory {
    private final MessageProcessingFilter messageProcessingFilter;
    private final ReplicationLinks replicationLinks;

    public ConsumerGroupFactory(final MessageProcessingFilter messageProcessingFilter,
            final ReplicationLinks replicationLinks) {

        this.messageProcessingFilter = messageProcessingFilter;
        this.replicationLinks = replicationLinks;
    }

    public ConsumerGroup create(final String consumerGroupName) {
        var consumerGroup = new ConsumerGroup(consumerGroupName, messageProcessingFilter, replicationLinks);
        return consumerGroup;
    }
}
