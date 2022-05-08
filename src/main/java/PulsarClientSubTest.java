
import org.apache.pulsar.client.api.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class PulsarClientSubTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarClientSubTest.class);

    private static final String PULSAR_BROKER_URL = "pulsar://localhost:6650";
    private static final String TOPIC_NAME = "test-topic";

    public static void main(String[] args) throws PulsarClientException {

        // Create client object
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(PULSAR_BROKER_URL)
                .build();

        // Create consumer on a topic with a subscription
        Consumer consumer = client.newConsumer()
                .topic(TOPIC_NAME)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("my-subscription")
                .subscribe();

        boolean receivedMsg = false;
        // Loop until a message is received
        do {
            // Block for up to 1 second for a message
            Message msg = consumer.receive(1, TimeUnit.SECONDS);

            if(msg != null){
                LOGGER.info("Message received: %s", new String(msg.getData()));

                // Acknowledge the message to remove it from the message backlog
                consumer.acknowledge(msg);

                receivedMsg = true;
            }

        } while (!receivedMsg);

        //Close the consumer
        consumer.close();

        // Close the client
        client.close();

    }
}
