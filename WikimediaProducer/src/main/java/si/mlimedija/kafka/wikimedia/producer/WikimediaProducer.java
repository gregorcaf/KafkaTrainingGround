package si.mlimedija.kafka.wikimedia.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducer {
    private static final Logger log = LoggerFactory.getLogger(WikimediaProducer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("I am a Kafka producer");

        String topic = "wikimedia.recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        String bootstrapServers = "X";

        // set producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"X\" password=\"X\";");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // high throughput producer congis
        properties.setProperty("batch.size", Integer.toString(32 * 1024));
        properties.setProperty("compression.type", "snappy");
        properties.setProperty("linger.ms", "20"); // small delay

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // EventHandler => handling events coming from the stream and sending them with producer to the topic
        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        // we produce for 5 minutes and then block the program
        TimeUnit.MINUTES.sleep(5);

    }
}