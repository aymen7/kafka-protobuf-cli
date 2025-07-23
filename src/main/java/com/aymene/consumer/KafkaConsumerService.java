package com.aymene.consumer;

import com.aymene.config.KafkaConsumerConfig;
import com.aymene.protobuf.PersonOuterClass.Person;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

public class KafkaConsumerService implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final KafkaConsumer<String, byte[]> consumer;

    public KafkaConsumerService(String bootstrapServers, String topic, String groupId) {
        this.consumer = KafkaConsumerConfig.createConsumer(bootstrapServers, groupId);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public void pollAndPrint(int timeoutMs) {
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(timeoutMs));
        for (ConsumerRecord<String, byte[]> consumerRecord : records) {
            try {
                Person person = Person.parseFrom(consumerRecord.value());
                logger.info("----- Received Person -----");
                logger.info("Name: {}", person.getName());
                logger.info("Id: {}", person.getId());
                if (person.hasEmail()) {
                    logger.info("Email: {}", person.getEmail());
                }
                logger.info("---------------------------");
            } catch (Exception e) {
                logger.error("Failed to parse protobuf message", e);
            }
        }
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }
}
