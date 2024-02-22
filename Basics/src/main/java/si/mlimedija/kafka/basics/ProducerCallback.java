package si.mlimedija.kafka.basics;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka producer");

        // create and set properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "X");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"X\" password=\"x\";");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // send multiple messages
        for (int i = 0; i < 10; i++) {
            String topic = "demo_java";
            String key = "id_" + i;
            String value = "Hello world " + i;

            // create producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            // send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record successfully  sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        log.info("Received new metadata \n" +
                                "Key: " + key + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });
        }


        // flush
        producer.flush();

        // close
        producer.close();
    }
}
