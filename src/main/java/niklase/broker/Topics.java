package niklase.broker;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Topics {
    private static final Logger logger = LoggerFactory.getLogger(Topics.class);

    private final ReplicationSender replicationSender;

    private HashMap<String, Set<ConsumerGroup>> consumerGroups = new HashMap<>();
    private Function<String, Boolean> shouldBeProcessed;
    private DeliveryPropagator deliveryPropagator;

    public Topics(final ReplicationSender replicationSender) {
        this.replicationSender = replicationSender;
    }

    public synchronized void subscribeConsumerGroupToTopic(final String topic, final String consumerGroupName,
            final boolean replicate) {
        if (replicate) {
            replicationSender.acceptSubscriptionRequest(topic, consumerGroupName);
        }
        var consumerGroupSetOfTopic = consumerGroups.get(topic);
        if (consumerGroupSetOfTopic == null) {
            var consumerGroupSet = new HashSet<ConsumerGroup>();
            consumerGroupSet.add(createConsumerGroup(consumerGroupName));
            consumerGroups.put(topic, consumerGroupSet);
        } else {
            if (consumerGroupSetOfTopic.contains(new ConsumerGroup(consumerGroupName))) {
                // already subscribed
            }
            consumerGroupSetOfTopic.add(createConsumerGroup(consumerGroupName));
        }
    }

    private ConsumerGroup createConsumerGroup(final String consumerGroupName) {
        var consumerGroup = new ConsumerGroup(consumerGroupName);
        if (shouldBeProcessed != null) {
            consumerGroup.setShouldBeProcessed(this.shouldBeProcessed);
            consumerGroup.setPropagateSuccessfulMessageDelivery(this.deliveryPropagator);
        }
        return consumerGroup;
    }

    public ConsumerGroup getConsumerGroupByName(final String consumerGroupName) {
        return consumerGroups.values().stream().flatMap(Collection::stream)
                .filter(consumerGroup -> consumerGroup.getName().equals(consumerGroupName)).findFirst().get();
    }

    public void accept(final String topic, final String message) {
        var envelope = "MESSAGE," + topic + "," + message;
        byTopic(topic).forEach(consumerGroup -> {
            replicationSender.acceptMessage(
                    "REPLICATED_MESSAGE," + consumerGroup.getName() + "," + topic + "," + message);
            consumerGroup.accept(envelope);
        });
    }

    private Set<ConsumerGroup> byTopic(final String topic) {
        var consumerGroupsOnTopic = this.consumerGroups.get(topic);
        return consumerGroupsOnTopic == null ? new HashSet<>() : consumerGroupsOnTopic;
    }

    public Set<ConsumerGroup> getAllConsumerGroups() {
        return consumerGroups.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
    }

    public Collection<ConsumerGroup> getConsumerGroupsSubscribedTo(final String topic) {
        return this.byTopic(topic);
    }

    public void tidy(final String topic) {
        this.byTopic(topic).removeIf(ConsumerGroup::isEmpty);
    }

    public void setMessageProcessingFilter(Function<String, Boolean> shouldBeProcessed) {
        this.shouldBeProcessed = shouldBeProcessed;
    }

    public void setPropagateSuccessfulMessageDelivery(DeliveryPropagator deliveryPropagator) {
        this.deliveryPropagator = deliveryPropagator;
    }

    public void deleteMessage(String topicName, String consumerGroup, final String message) {
        //TODO handle case topic does not exist
        this.getConsumerGroupByName(consumerGroup).delete("MESSAGE," + topicName + "," + message);
    }

    public void flush(final String consumerGroupName) {
        this.getConsumerGroupByName(consumerGroupName).flush();
    }

    public void createConsumerGroupForTopic(final String topic, final String consumerGroup) {
        this.subscribeConsumerGroupToTopic(topic, consumerGroup, true);
    }

    public void storeInConsumerGroup(final String consumerGroup, final String envelope) {
        this.getConsumerGroupByName(consumerGroup).accept(envelope);
    }
}
