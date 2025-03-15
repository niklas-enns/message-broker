package niklase.broker;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProcessingFilter {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessingFilter.class);
    private int countOfDistributorNodes = 1;

    Function<String, Boolean> f = (envelope) -> true; //default process all messages
    private String messageDistributionRule = "all";

    boolean shouldBeProcessed(String envelope) {
        return f.apply(envelope);
    }

    void setModuloRemainder(int moduloRemainder, final int countOfDistributorNodes) {
        this.countOfDistributorNodes = countOfDistributorNodes;
        messageDistributionRule = "All messages % " + this.countOfDistributorNodes + " == " + moduloRemainder;
        logger.info("I will process " + messageDistributionRule);
        f = (envelope) -> {
            var calc = Math.abs(envelope.hashCode() % this.countOfDistributorNodes);
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
