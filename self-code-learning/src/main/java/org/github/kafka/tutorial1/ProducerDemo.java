package org.github.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello world");

        String bootstrapServer="127.0.0.1:9092";

        //create producer propeties
        Properties properties =new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create new producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create producer records
        ProducerRecord<String,String> record =new ProducerRecord<String, String>("first_topic","Hello i'm from java");

        // Sending data to producer
        producer.send(record);

        // flush data
        producer.flush();

        //close kafka
        producer.close();
    }
}
