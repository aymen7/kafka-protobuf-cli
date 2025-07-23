package com.aymene.producer;

import com.aymene.config.KafkaProducerConfig;
import com.aymene.producer.converter.JsonToProtobufConverter;
import com.aymene.protobuf.PersonOuterClass;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.Future;

public class KafkaProtobufProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProtobufProducer.class);

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;

    public KafkaProtobufProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        this.producer = KafkaProducerConfig.createProducer(bootstrapServers);
    }

    public void send(PersonOuterClass.Person person) throws Exception {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                topic,
                String.valueOf(person.getId()),
                person.toByteArray()
        );
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get();

        logger.info("Message sent to topic {} partition {} offset {}",
                metadata.topic(), metadata.partition(), metadata.offset());
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    public void sendMessageFromJsonFile(String jsonFilePath) throws Exception {
        File jsonFile = new File(jsonFilePath);
        PersonOuterClass.Person person = JsonToProtobufConverter.fromJsonFile(jsonFile);
        send(person);
    }
}
