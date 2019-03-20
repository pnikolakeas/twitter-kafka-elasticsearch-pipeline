package com.github.pnikolakeas.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.github.pnikolakeas.kafka.util.KafkaProperties.*;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private final String consumerKey    ="Pu8hEiLAkG7crZBT1RGRVmSK3";
    private final String consumerSecret ="AnVPLnylB7Fls26Ds7MNZTcNJvSfBaojJKjS5Bvcw3XGTy1Urp";
    private final String token          = "3364842525-IrTgvEirRjbgg01aKr1kVqBFw9nOi3cTCSJiYko";
    private final String secret         = "AHiDKPMjxmu9B6gVgITd1zpz0nganLLeuqp6345h4kibr";

    List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");

    private TwitterProducer() {}

    public static void main(String[] args) {
        TwitterProducer twitterProducer = new TwitterProducer();
        twitterProducer.run();
    }

    public void run() {

        logger.info("Setup");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue(1000);

        // Create a twitter client
        Client twitterClient = createTwitterClient(msgQueue);
        // Attempt to establish a connection
        twitterClient.connect();

        // Create a Kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application..");
            logger.info("Shutting down client from twitter..");
            twitterClient.stop();
            logger.info("Closing producer..");
            producer.close();
            logger.info("done!");
        }));

        // loop to send tweets to Kafka
        // on a different thread, or multiple different threads....
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
            something(Optional.ofNullable(msg), producer);
        }

        logger.info("End of Application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(PRODUCER_BOOTSTRAP_SERVER.getDescription(), PRODUCER_BOOTSTRAP_SERVER.getValue());
        properties.setProperty(KEY_SERIALIZER.getDescription(), KEY_SERIALIZER.getValue());
        properties.setProperty(VALUE_SERIALIZER.getDescription(), VALUE_SERIALIZER.getValue());
        properties.setProperty(ACKS_CONFIGURATION.getDescription(), ACKS_CONFIGURATION.getValue());

        // Make Producer Safer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");                   // Prevent duplicates, by using producer request id.
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");                                  // Awaits ACKs from the leader and all in-sync-replicas
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)); // How many retries should be done until ACKs retrieved
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");          // KAFKA 2.1 >= 1.1 so we can keep this 5. Use 1 otherwise. Preserves ordering. 5 messages individually sent at the same time. per partition.

        // High Throughput Producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");                   // Compression algorithm to utilize
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");                              // How much delay to introduce to my producer
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));     // Define the batch size

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);
        return producer;
    }

    private void something(Optional<String> msg, KafkaProducer<String, String> producer) {
        Optional<ProducerRecord> producerRecord = msg.map(o -> new ProducerRecord("twitter_tweets", null, o));
        producerRecord.ifPresent(value -> producer.send(value, (recordMetadata, e) -> {
            if(e != null) {
                logger.error("Something bad happened!", e);
            }
        }));
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}
