package com.github.pnikolakeas.kafka.producer;

import static com.github.pnikolakeas.kafka.util.KafkaProperties.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(PRODUCER_BOOTSTRAP_SERVER.getDescription(), PRODUCER_BOOTSTRAP_SERVER.getValue());
        properties.setProperty(KEY_SERIALIZER.getDescription(), KEY_SERIALIZER.getValue());
        properties.setProperty(VALUE_SERIALIZER.getDescription(), VALUE_SERIALIZER.getValue());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        // Create Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord("first_topic", "data send from producer");

        // Send Data - Asynchronous
        producer.send(producerRecord);

        // Flush Data
        producer.flush();

        // Flush and Close Producer
        producer.close();

    }
}
