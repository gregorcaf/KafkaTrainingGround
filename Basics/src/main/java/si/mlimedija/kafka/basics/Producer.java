package si.mlimedija.kafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka producer");

        // create Producer Properties => configuration of producer
        Properties properties = new Properties();

        // connect to Conduktor playground
        properties.setProperty("bootstrap.servers", "X");
        properties.setProperty("security.protocol", "SASL_SSL");
        // sasl.jaas.config => include the last semicolon ;
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"X\" password=\"X\";");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");

        // set Producer properties => serializer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a Producer Record => record that we are going to send to Kafka
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Maribor");

        // send data
        producer.send(producerRecord);

        // flush the producer => tell the producer to send all the data and block until done -- synchronous
        producer.flush();

        // flush and close the producer => close() again calls flush()
        producer.close();
    }
}