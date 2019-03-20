package com.github.pnikolakeas.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.github.pnikolakeas.kafka.util.KafkaProperties.*;

public class ConsumerDemo {

    static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

    public static void main(String[] args) {

        // Create Consumer Configs
        Properties properties = new Properties();
        properties.setProperty(CONSUMER_BOOTSTRAP_SERVER.getDescription(), CONSUMER_BOOTSTRAP_SERVER.getValue());
        properties.setProperty(KEY_DESERIALIZER.getDescription(), KEY_DESERIALIZER.getValue());
        properties.setProperty(VALUE_DESERIALIZER.getDescription(), VALUE_DESERIALIZER.getValue());
        properties.setProperty(GROUP_ID.getDescription(), GROUP_ID.getValue());
        properties.setProperty(AUTO_OFFSET_RESET.getDescription(), AUTO_OFFSET_RESET.getValue());

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);

        // Subscribe Consumer to our topic(s)
        // consumer.subscribe(Arrays.asList("first_topic", "second_topic")); // Subscribe to more than one topic
        consumer.subscribe(Collections.singleton(TOPIC.getValue()));         // Subscribe to only one topic

        // Poll for new data
        while(true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {

                StringBuilder sb = new StringBuilder();
                sb.append("Consumed new record. \n")
                        .append("Key: ")
                        .append(consumerRecord.key())
                        .append("\n")
                        .append("Value: ")
                        .append(consumerRecord.value())
                        .append("\n")
                        .append("Partition: ")
                        .append(consumerRecord.partition())
                        .append("\n")
                        .append("Offset: ")
                        .append(consumerRecord.offset());
                logger.info(sb.toString());
            }
        }
    }
}
