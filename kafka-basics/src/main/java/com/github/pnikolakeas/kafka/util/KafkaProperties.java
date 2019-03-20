package com.github.pnikolakeas.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public enum KafkaProperties {

    PRODUCER_BOOTSTRAP_SERVER(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092"),
    CONSUMER_BOOTSTRAP_SERVER(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092"),
    KEY_SERIALIZER(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
    VALUE_SERIALIZER(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
    KEY_DESERIALIZER(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
    VALUE_DESERIALIZER(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
    GROUP_ID(GROUP_ID_CONFIG, "my-forth-application"),
    TOPIC("topic", "first_topic"),
    AUTO_OFFSET_RESET(AUTO_OFFSET_RESET_CONFIG, "earliest"), // earliest: from beginning | latest: any new onwards | none: throws errors if there's no offsets being saved.
    ACKS_CONFIGURATION(ACKS_CONFIG, "1");

    private String description;
    private String value;

    public String getDescription() {
        return description;
    }

    public String getValue() {
        return value;
    }

    KafkaProperties(String description, String value) {
        this.description = description;
        this.value = value;
    }
}
