package com.github.MarioBezerra.tutorial1.FirstSteps.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static final String bootstrapServer = "127.0.0.1:9092";
    public static final String topic = "second_topic";

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        //create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly used to replay data of fetch a specific message

        //assign
        TopicPartition partitionToRead = new TopicPartition(topic, 0);
        long offSetToReadFrom = 5L;
        consumer.assign(Arrays.asList(partitionToRead));

        //seek
        consumer.seek(partitionToRead, offSetToReadFrom);

        final int numberOfMessagesToRead = 5;
        boolean keepReading = true;
        int numberOfMessagesRead = 0;

        //poll for new data
        while (keepReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesRead++;
                logger.info("\nKey: " + record.key() + "\n" +
                            "Value: " + record.value() + "\n" +
                            "Topic: " + record.topic() + "\n" +
                            "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset() + "\n");
                if (numberOfMessagesRead >= numberOfMessagesToRead){
                    keepReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the application");
    }
}
