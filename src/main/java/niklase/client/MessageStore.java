package niklase.client;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MessageStore {
    private final Map<String, List<String>> messages = new LinkedHashMap<>();

    public List<String> get(final String topic) {
        var messages = this.messages.get(topic);
        return messages == null ? List.of() : messages;
    }

    public void init(final String topic) {
        this.messages.put(topic, new LinkedList<>());
    }

    public void add(final String topic, final String message) {
        var messages = this.messages.get(topic);
        messages.add(message);
    }

    public void deleteAllMessages() {
        this.messages.forEach((s, strings) -> {
            strings.clear();
        });
    }
}
