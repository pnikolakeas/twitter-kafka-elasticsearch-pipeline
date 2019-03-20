package com.github.pnikolakeas.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.github.pnikolakeas.kafka.util.KafkaProperties.*;

public class ConsumerDemoAssignSeek {

    static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

    public static void main(String[] args) {

        // Create Consumer Configs
        Properties properties = new Properties();
        properties.setProperty(CONSUMER_BOOTSTRAP_SERVER.getDescription(), CONSUMER_BOOTSTRAP_SERVER.getValue());
        properties.setProperty(KEY_DESERIALIZER.getDescription(), KEY_DESERIALIZER.getValue());
        properties.setProperty(VALUE_DESERIALIZER.getDescription(), VALUE_DESERIALIZER.getValue());
        properties.setProperty(AUTO_OFFSET_RESET.getDescription(), AUTO_OFFSET_RESET.getValue());

        // Create Consumer without A GROUP ID!!!
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);

        // Assign and Seek are mostly used to replay data or fetch a specific message

        // Assign => To a specific topic partition
        TopicPartition partitionToReadFrom = new TopicPartition(TOPIC.getValue(), 0); // Reading the First Partition only of the first_topic
        consumer.assign(Collections.singleton(partitionToReadFrom));

        // Seek
        long offsetToReadFrom=15L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        boolean keepOnReading = true;
        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;

        // Poll for new data
        while(keepOnReading) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                StringBuilder sb = new StringBuilder();
                sb.append("Consumed new record. \n")
                        .append("Key: ")
                        .append(consumerRecord.key())
                        .append("Value: ")
                        .append(consumerRecord.value())
                        .append("Partition: ")
                        .append(consumerRecord.partition())
                        .append("Offset: ")
                        .append(consumerRecord.offset());
                logger.info(sb.toString());
                numberOfMessagesReadSoFar++;
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false; // To exit the while loop
                    break;                 // To exit the for loop
                }
            }
        }

        logger.info("Exiting the application");
    }
}
