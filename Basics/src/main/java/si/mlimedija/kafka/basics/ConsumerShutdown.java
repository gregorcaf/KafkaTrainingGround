package si.mlimedija.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka consumer");

        String topic = "demo_java";
        String groupId = "my-java-application";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "X");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"X\" password=\"X\";");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId); // set consumer group id
        properties.setProperty("auto.offset.reset", "earliest"); // none/earliest/latest

        // incremental rebalancing strategy
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()");
                consumer.wakeup(); // consumer.poll will at one point throw a wakeup exception

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                 // at some point consumer.poll() will throw an exception
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + " | Value: " + record.value());
                    log.info("Partition: " + record.partition() + " | Offset: " + record.offset());
                    log.info("Timestamp: " + record.timestamp());
                }
            }
        } catch (WakeupException e) {
            // we are expecting this exception
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            // in case of unexpected exception
            log.error("Unexpected exception in the consumer", e);
        } finally {
            // close the consumer, this will also commit the offsets
            consumer.close();
            log.info("The consumer will gracefully shutdown");
        }
    }
}
