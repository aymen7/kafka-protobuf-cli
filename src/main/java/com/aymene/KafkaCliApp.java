package com.aymene;

import com.aymene.consumer.KafkaConsumerApp;
import com.aymene.producer.KafkaProtobufProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaCliApp {

    private static final Logger logger = LoggerFactory.getLogger(KafkaCliApp.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            printUsageAndExit();
        }

        String mode = args[0].toLowerCase();

        switch (mode) {
            case "produce":
                runProducer(args);
                break;

            case "consume":
                runConsumer(args);
                break;

            default:
                logger.error("Unknown mode: {}. Must be 'produce' or 'consume'", mode);
                printUsageAndExit();
        }
    }

    private static void runProducer(String[] args) {
        if (args.length != 4) {
            logger.error("Invalid arguments for produce mode.");
            logger.info("Usage: produce <json-file-path> <topic> <bootstrap-server>");
            System.exit(1);
        }
        String jsonFile = args[1];
        String topic = args[2];
        String bootstrapServer = args[3];

        try {
            KafkaProtobufProducer producer = new KafkaProtobufProducer(bootstrapServer, topic);
            producer.sendMessageFromJsonFile(jsonFile);
            producer.close();
            logger.info("Message sent successfully.");
        } catch (Exception e) {
            logger.error("Failed to send message", e);
            System.exit(1);
        }
    }

    private static void runConsumer(String[] args) {
        if (args.length != 4) {
            logger.error("Invalid arguments for consume mode.");
            logger.info("Usage: consume <topic> <bootstrap-server> <group-id>");
            System.exit(1);
        }
        String topic = args[1];
        String bootstrapServer = args[2];
        String groupId = args[3];

        try (KafkaConsumerApp consumerApp = new KafkaConsumerApp(bootstrapServer, topic, groupId)) {
            logger.info("Starting consumer. Press Ctrl+C to exit...");
            consumerApp.pollAndPrintContinuously();
        } catch (Exception e) {
            logger.error("Consumer failed", e);
            System.exit(1);
        }
    }

    private static void printUsageAndExit() {
        logger.info("Usage:");
        logger.info("  produce <json-file-path> <topic> <bootstrap-server>");
        logger.info("  consume <topic> <bootstrap-server> <group-id>");
        System.exit(1);
    }
}
