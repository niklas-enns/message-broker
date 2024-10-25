package niklase.broker;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Topics {
    private static final Logger logger = LoggerFactory.getLogger(Topics.class);

    private final ReplicationLinks replicationLinks;
    private MessageProcessingFilter messageProcessingFilter;

    private HashMap<String, Set<ConsumerGroup>> consumerGroups = new HashMap<>();
    private DeliveryPropagator deliveryPropagator;

    public Topics(final ReplicationLinks replicationLinks, final MessageProcessingFilter messageProcessingFilter) {
        this.replicationLinks = replicationLinks;
        this.messageProcessingFilter = messageProcessingFilter;
    }

    public synchronized void subscribeConsumerGroupToTopic(final String topic, final String consumerGroupName,
            final boolean replicate) {
        if (replicate) {
            replicationLinks.acceptSubscriptionRequest(topic, consumerGroupName);
        }
        var consumerGroupSetOfTopic = consumerGroups.get(topic);
        if (consumerGroupSetOfTopic == null) {
            var consumerGroupSet = new HashSet<ConsumerGroup>();
            consumerGroupSet.add(createConsumerGroup(consumerGroupName));
            consumerGroups.put(topic, consumerGroupSet);
        } else {
            if (consumerGroupSetOfTopic.contains(new ConsumerGroup(consumerGroupName, messageProcessingFilter))) {
                // already subscribed
            }
            consumerGroupSetOfTopic.add(createConsumerGroup(consumerGroupName));
        }
    }

    private ConsumerGroup createConsumerGroup(final String consumerGroupName) {
        var consumerGroup = new ConsumerGroup(consumerGroupName, messageProcessingFilter);
        consumerGroup.setPropagateSuccessfulMessageDelivery(this.deliveryPropagator);
        return consumerGroup;
    }

    public ConsumerGroup getConsumerGroupByName(final String consumerGroupName) {
        return consumerGroups.values().stream().flatMap(Collection::stream)
                .filter(consumerGroup -> consumerGroup.getName().equals(consumerGroupName)).findFirst().get();
    }

    public void accept(final String topic, final String message) {
        var envelope = "MESSAGE," + topic + "," + message;
        byTopic(topic).forEach(consumerGroup -> {
            replicationLinks.acceptMessage(
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
