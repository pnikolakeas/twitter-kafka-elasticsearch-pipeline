package com.github.pnikolakeas.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.github.pnikolakeas.kafka.util.KafkaProperties.KEY_SERIALIZER;
import static com.github.pnikolakeas.kafka.util.KafkaProperties.PRODUCER_BOOTSTRAP_SERVER;
import static com.github.pnikolakeas.kafka.util.KafkaProperties.VALUE_SERIALIZER;

public class ProducerDemoWithCallback {

    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(PRODUCER_BOOTSTRAP_SERVER.getDescription(), PRODUCER_BOOTSTRAP_SERVER.getValue());
        properties.setProperty(KEY_SERIALIZER.getDescription(), KEY_SERIALIZER.getValue());
        properties.setProperty(VALUE_SERIALIZER.getDescription(), VALUE_SERIALIZER.getValue());
        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        Callback callback = ProducerDemoWithCallback::onCompletion;

        for(int k = 0; k<=10; k++) {
            // Create Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord("first_topic", "data send from producer with callback " + Integer.toString(k));
            // Send Data - Asynchronous
            producer.send(producerRecord, callback);
        }

        // Flush Data
        producer.flush();

        // Flush and Close Producer
        producer.close();

    }

    private static void onCompletion(RecordMetadata recordMetadata, Exception e) {
        // Executes every time is successfully sent or an exception is thrown
        if (e != null) {
            logger.error("Error while producing", e);
            return;
        }
        // the record was sent successfully
        StringBuilder sb = new StringBuilder();
        sb.append("Received new metadata. \n")
                .append("Topic: ")
                .append(recordMetadata.topic())
                .append("\n")
                .append("Partition: ")
                .append(recordMetadata.partition())
                .append("\n")
                .append("Offset: ")
                .append(recordMetadata.offset())
                .append("\n")
                .append("Timestamp: ")
                .append(recordMetadata.timestamp());

        logger.info(sb.toString());
    }
}
