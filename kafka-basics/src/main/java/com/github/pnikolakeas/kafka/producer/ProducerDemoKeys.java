package com.github.pnikolakeas.kafka.producer;

import static com.github.pnikolakeas.kafka.util.KafkaProperties.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(PRODUCER_BOOTSTRAP_SERVER.getDescription(), PRODUCER_BOOTSTRAP_SERVER.getValue());
        properties.setProperty(KEY_SERIALIZER.getDescription(), KEY_SERIALIZER.getValue());
        properties.setProperty(VALUE_SERIALIZER.getDescription(), VALUE_SERIALIZER.getValue());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        Callback callback = ProducerDemoKeys::onCompletion;

        for(int k = 0; k<=10; k++) {

            final String topic = "first_topic";
            final String value = "hello world " + Integer.toString(k);
            final String key   = "id_" + Integer.toString(k);

            // Create Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord(topic,key,value);

            logger.info("Key: " + key); // log the key => Always the same key goes to the same partition for a fix number of partitions
            // id_0  Partition: 1
            // id_1  Partition: 0
            // id_2  Partition: 2
            // id_3  Partition: 0
            // id_4  Partition: 2
            // id_5  Partition: 2
            // id_6  Partition: 0
            // id_7  Partition: 2
            // id_8  Partition: 1
            // id_9  Partition: 2
            // id_10 Partition: 2

            // Send Data - Asynchronous
            producer.send(producerRecord, callback).get(); // block the .send() to make it synchronous - do not do this in production!
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
