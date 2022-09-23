import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.slf4j.event.Level;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
//import java.util.stream.IntStream;

public class PulsarClientPubTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarClientPubTest.class);

    //private static final String PULSAR_BROKER_URL = "pulsar://localhost:6650";
    private static final String PULSAR_BROKER_URL = "<Pulsar broker URL>";
    private static final String TOPIC_NAME = "persistent://<full topic name>";
    private static final String CONNECTION_TOKEN = "token";

    public static void main(String[] args) throws IOException {

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(PULSAR_BROKER_URL)
                .authentication(AuthenticationFactory.token(CONNECTION_TOKEN))
                .build();

        LOGGER.info("client connected at: " + PULSAR_BROKER_URL);

        Producer producer = client.newProducer()
                .topic(TOPIC_NAME)
                .create();

        LOGGER.info("producer created for the topic: " + TOPIC_NAME);

        //IntStream.range(1,5).forEach(i -> {
        //    String content = String.format("hello-pulsar-%d", i);

        //    Message<byte[]> msg = TypedMessageBuilder.
        //            .setContent(content.getBytes())
        //            .build();

        try {
            /** MessageId messageId = producer.newMessage()
               .key("test_key")
               .value("test_value")
               .send();
             */

            producer.send("Emitting the message".getBytes(StandardCharsets.UTF_8));
            LOGGER.info("Published the message");
        } catch (PulsarClientException e) {
            LOGGER.error(e.toString());
        }

        producer.close();
        client.close();
    }

}

