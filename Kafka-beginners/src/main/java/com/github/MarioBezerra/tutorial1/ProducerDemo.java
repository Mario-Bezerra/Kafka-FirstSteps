package com.github.MarioBezerra.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    private static final String bootstrapServerAdress = "127.0.0.1:9092";

    public static void main(String[] args) {

        //create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerAdress);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world");

        //send data
        producer.send(record);
        //flush data
        producer.flush();
        //close producer
        producer.close();
    }
}
