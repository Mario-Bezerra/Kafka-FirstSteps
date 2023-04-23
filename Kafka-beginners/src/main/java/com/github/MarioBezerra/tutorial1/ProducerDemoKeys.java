package com.github.MarioBezerra.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final String bootstrapServerAdress = "127.0.0.1:9092";

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        //create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerAdress);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0 ; i < 10 ; i++){
        //create a Producer Record
        String topic = "second_topic";
        String value = "Hello world" + Integer.toString(i);
        String key = "id_" + Integer.toString(i); //providing a key guarantees that the same key always go to the same partition

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        logger.info("Key: " + key); //log the key

        //send data - asynchronous
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes every time a record is successfully sent or an exception is throw
                if (e == null){
                    logger.info("Received new metadata.\n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error wthile producing ", e);
                }
            }
        });
        }
        //flush data
        producer.flush();
        //close producer
        producer.close();
    }
}
