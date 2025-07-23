package com.aymene.consumer;

import com.aymene.config.KafkaConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

public class KafkaConsumerApp implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerApp.class);

    private final KafkaConsumer<String, byte[]> consumer;
    private final String topic;
    private volatile boolean running = true;

    public KafkaConsumerApp(String bootstrapServers, String topic, String groupId) {
        this.topic = topic;
        this.consumer = KafkaConsumerConfig.createConsumer(bootstrapServers, groupId);
        this.consumer.subscribe(Collections.singletonList(topic));
        logger.info("Subscribed to topic {}", topic);
    }

    public void pollAndPrintContinuously() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown requested, closing consumer...");
            running = false;
            close();
        }));

        while (running) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, byte[]> record : records) {
                logger.info("Received message at offset {} partition {}:", record.offset(), record.partition());
                printMessage(record.value());
            }
        }
    }

    private void printMessage(byte[] data) {
        try {
            com.aymene.protobuf.PersonOuterClass.Person person =
                    com.aymene.protobuf.PersonOuterClass.Person.parseFrom(data);
            logger.info("Person Name: {}", person.getName());
            logger.info("Person Id: {}", person.getId());
            logger.info("Person Email: {}", person.getEmail());
            logger.info("---------------------------");
        } catch (Exception e) {
            logger.error("Failed to parse protobuf message", e);
        }
    }

    @Override
    public void close() {
        try {
            consumer.wakeup();
            consumer.close();
            logger.info("Kafka consumer closed");
        } catch (Exception e) {
            logger.error("Error closing consumer", e);
        }
    }
}
