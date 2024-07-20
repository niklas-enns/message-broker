package niklase.broker;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class Topics {
    private HashMap<String, Set<ConsumerGroup>> consumerGroups = new HashMap<>();

    public synchronized void subscribeConsumerGroupToTopic(final String topic, final String consumerGroupName) {
        var consumerGroupSetOfTopic = consumerGroups.get(topic);
        if (consumerGroupSetOfTopic == null) {
            var consumerGroupSet = new HashSet<ConsumerGroup>();
            consumerGroupSet.add(new ConsumerGroup(consumerGroupName));
            consumerGroups.put(topic, consumerGroupSet);
        } else {
            if (consumerGroupSetOfTopic.contains(new ConsumerGroup(consumerGroupName))) {
                // already subscribed
            }
            consumerGroupSetOfTopic.add(new ConsumerGroup(consumerGroupName));
        }
    }

    public ConsumerGroup getConsumerGroupByName(final String consumerGroupName) {
        return consumerGroups.values().stream().flatMap(Collection::stream)
                .filter(consumerGroup -> consumerGroup.getName().equals(consumerGroupName)).findFirst().get();
    }


    public void accept(final String topic, final String message) {
        var envelope = "MESSAGE," + topic + "," + message;
        byTopic(topic).forEach(consumerGroup -> {
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

}
