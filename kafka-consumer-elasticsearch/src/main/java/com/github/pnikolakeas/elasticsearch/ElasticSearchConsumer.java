package com.github.pnikolakeas.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.github.pnikolakeas.kafka.util.KafkaProperties.*;

public class ElasticSearchConsumer {

    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static void main(String[] args) throws IOException {

        // Create a REST client for elastic search
        RestHighLevelClient client = createClient();

        // Create a Kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        // Poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records) {

                // We do insert data into ElasticSearch

                // Data example
                // String jsonString = "{ \"foo\": \"bar\" }";
                String jsonString = record.value();

                // Insert some data [String index, String type]
                IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);

                // Run it to insert data to Elastic Search
                IndexResponse index = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = index.getId();
                logger.info(id);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // Close the client gracefully
        // client.close();
    }

    public static RestHighLevelClient createClient() {

        final String hostname = "kafka-course-970250227.eu-central-1.bonsaisearch.net";
        final String username = "51j9f2sglc";
        final String password = "putb6842p0";
        final int port        = 443;
        final String scheme   = "https";

        BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        basicCredentialsProvider.setCredentials(AuthScope.ANY,
                                                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, port, scheme))
                                              .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                                                                           httpAsyncClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider)
                                              );

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(builder);
        return restHighLevelClient;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {

        final String bootstrapServers = "127.0.0.1:9092";
        final String groupId = "kafka-demo-elasticsearch";

        // Create Consumer Configs
        Properties properties = new Properties();
        properties.setProperty(CONSUMER_BOOTSTRAP_SERVER.getDescription(), bootstrapServers);
        properties.setProperty(KEY_DESERIALIZER.getDescription(), KEY_DESERIALIZER.getValue());
        properties.setProperty(VALUE_DESERIALIZER.getDescription(), VALUE_DESERIALIZER.getValue());
        properties.setProperty(GROUP_ID.getDescription(), groupId);
        properties.setProperty(AUTO_OFFSET_RESET.getDescription(), AUTO_OFFSET_RESET.getValue());

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);

        // Subscribe Consumer to our topic(s)
        // consumer.subscribe(Arrays.asList("first_topic", "second_topic")); // Subscribe to more than one topic
        consumer.subscribe(Collections.singleton(topic));                    // Subscribe to only one topic

        return consumer;
    }
}
