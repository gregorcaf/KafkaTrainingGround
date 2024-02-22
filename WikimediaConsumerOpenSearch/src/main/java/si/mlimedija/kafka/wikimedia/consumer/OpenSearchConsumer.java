package si.mlimedija.kafka.wikimedia.consumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static RestHighLevelClient createOpenSearchClient() {
        String connectionString = "X";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connectionUri = URI.create(connectionString);
        // extract login information if it exists
        String userInfo = connectionUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connectionUri.getHost(), connectionUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connectionUri.getHost(), connectionUri.getPort(), connectionUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }
        return restHighLevelClient;
    }


    private static KafkaConsumer<String, String> createKafkaConsumer() {

        String bootstrapServers = "clear-mako-11225-eu2-kafka.upstash.io:9092";
        String groupId = "consumer-opensearch";

        Properties properties = new Properties();

        // set producer properties
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Y2xlYXItbWFrby0xMTIyNSR5RGCFi36W8OlNEyn2TsxBM8mYeXT-8ZW38Kx0xjQ\" password=\"NTQ5NWM0MTEtZDQ5Ni00MGQzLTlkYTQtNzY4YjA4MjBhMWZm\";");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");

        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId); // set consumer group id
        properties.setProperty("auto.offset.reset", "latest"); // none/earliest/latest

        return new KafkaConsumer<>(properties);
    }


    public static void main(String[] args) throws IOException {

        String topic = "wikimedia.recentchange";

        // create OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create kafka client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // create index on OpenSearch if it doesn't already exist
        try (openSearchClient; consumer) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            // check if index already exists
            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Wikimedia index has been created");
            } else {
                log.info("Wikimedia index already exists");
            }
        }

        // subscribe to the topic
        consumer.subscribe(Arrays.asList(topic));

        // main code logic
        while (true) {
            log.info("Polling");

            // poll the data
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

            int recordCount = records.count();
            log.info("Received " + recordCount + " record(s)");

            // send the record into OpenSearch
            for (ConsumerRecord<String, String> record : records) {
                IndexRequest indexRequest = new IndexRequest("wikimedia")
                        .source(record.value(), XContentType.JSON);

                IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                log.info(response.getId());
            }
        }

        // close things
    }
}