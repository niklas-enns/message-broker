package niklase.broker;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProcessingFilter {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessingFilter.class);

    Function<String, Boolean> f = (envelope) -> true; //default process all messages

    boolean shouldBeProcessed(String envelope) {
        return f.apply(envelope);
    }

    void setModuloRemainder(int moduloRemainder) {
        f = (envelope) -> {
            if (Math.abs(envelope.hashCode() % 2) == moduloRemainder) {
                logger.info("Processing message {} with hashCode % 2 = {} and configured moduloRemainder {}", envelope,
                        envelope.hashCode() % 2, moduloRemainder);
                return true;
            }
            logger.info("Skipping message {} with hashCode % 2 = {} and configured moduloRemainder {}", envelope,
                    envelope.hashCode() % 2, moduloRemainder);
            return false;
        };
    }
}
