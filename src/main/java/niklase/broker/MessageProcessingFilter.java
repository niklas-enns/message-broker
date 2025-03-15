package niklase.broker;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProcessingFilter {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessingFilter.class);
    public static final int COUNT_OF_MESSAGE_DISTRIBUTOR_NODES = 2; //TODO make this dynamic

    Function<String, Boolean> f = (envelope) -> true; //default process all messages
    private String messageDistributionRule = "all";

    boolean shouldBeProcessed(String envelope) {
        return f.apply(envelope);
    }

    void setModuloRemainder(int moduloRemainder) {
        messageDistributionRule = "All messages % " + COUNT_OF_MESSAGE_DISTRIBUTOR_NODES + " == " + moduloRemainder;
        logger.info("I will process " + messageDistributionRule);
        f = (envelope) -> {
            var calc = Math.abs(envelope.hashCode() % COUNT_OF_MESSAGE_DISTRIBUTOR_NODES);
            if (calc == moduloRemainder) {
                logger.info("Processing message {} with hashCode % 2 = {} and configured moduloRemainder {}", envelope,
                        calc, moduloRemainder);
                return true;
            }
            logger.info("Skipping message {} with hashCode % 2 = {} and configured moduloRemainder {}", envelope,
                    calc, moduloRemainder);
            return false;
        };
    }

    public String getMessageDistributionRule() {
        return this.messageDistributionRule;
    }

}
