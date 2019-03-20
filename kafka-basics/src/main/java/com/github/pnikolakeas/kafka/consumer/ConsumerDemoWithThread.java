package com.github.pnikolakeas.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.github.pnikolakeas.kafka.util.KafkaProperties.*;

public class ConsumerDemoWithThread {

    private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

    private ConsumerDemoWithThread() { }

    public static void main(String[] args) {
        ConsumerDemoWithThread consumerDemoWithThread = new ConsumerDemoWithThread();
        consumerDemoWithThread.run();
    }

    private void run() {

        // Latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // Create the Consumer Runnable
        logger.info("Creating the consumer thread!");
        Runnable runnable = new ConsumerRunnable(latch);

        // Start the Thread
        Thread thread = new Thread(runnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) runnable).shutdown();
            try {
                latch.await();
            } catch(Exception e) {
                e.printStackTrace();
            } finally {
                logger.info("Thread-2 is CLOSING!");
            }
        }));

        try{
            latch.await();
        } catch (Exception e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is stopping..");
        }

        logger.info("Main Thread is CLOSING!");
    }

    class ConsumerRunnable implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        private CountDownLatch latch;
        private KafkaConsumer consumer;

        ConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;
            this.consumer = new KafkaConsumer(getConsumerProperties());
            // Subscribe Consumer to our topic(s)
            // consumer.subscribe(Arrays.asList("first_topic", "second_topic")); // Subscribe to MORE THAN ONE topic
            consumer.subscribe(Collections.singleton(TOPIC.getValue()));         // Subscribe to ONLY ONE topic
        }

        @Override
        public void run() {
            try {
                while(true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        StringBuilder sb = getStringBuilder(consumerRecord);
                        logger.info(sb.toString());
                    }
                }
            } catch (WakeupException e) {
                logger.error("Received shutdown signal!");
            } finally {
                // Tell the main thread to continue
                logger.info("Latch count down! Tell the main to continue..");
                latch.countDown();

                logger.info("Thread-1 is CLOSING!");
                consumer.close();
            }

        }

         private void shutdown() {
            // The wakeup() method is a special method to interrupt consumer.poll()
            // It will throw the exception WakeUpException
            this.consumer.wakeup();
        }
    }

    /**
     * Creates the basic logging logic
     * @param consumerRecord
     * @return
     */
    private StringBuilder getStringBuilder(ConsumerRecord<String, String> consumerRecord) {
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
        return sb;
    }

    /**
     * Helper method for Consumer Properties initialization
     * @return Properties
     */
    private Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(CONSUMER_BOOTSTRAP_SERVER.getDescription(), CONSUMER_BOOTSTRAP_SERVER.getValue());
        properties.setProperty(KEY_DESERIALIZER.getDescription(), KEY_DESERIALIZER.getValue());
        properties.setProperty(VALUE_DESERIALIZER.getDescription(), VALUE_DESERIALIZER.getValue());
        properties.setProperty(GROUP_ID.getDescription(), GROUP_ID.getValue());
        properties.setProperty(AUTO_OFFSET_RESET.getDescription(), AUTO_OFFSET_RESET.getValue());
        return properties;
    }
}
