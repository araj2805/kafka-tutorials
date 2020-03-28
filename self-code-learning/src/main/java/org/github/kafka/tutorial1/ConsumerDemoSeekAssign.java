package org.github.kafka.tutorial1;

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

public class ConsumerDemoSeekAssign {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoSeekAssign.class.getName());

        String bootstrapServer = "127.0.0.1:9092";

        String topic = "first_topic";


        // Setting Property
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);


        // assign and seek are mostly used to replay data or fetch a specific message

        // assign
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        Long offsetToReadFrom = 15l;
        consumer.assign(Arrays.asList(topicPartition));

        //seek
        consumer.seek(topicPartition, offsetToReadFrom);

        int numberOfMessageToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessageSoFar = 0;
        //poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessageSoFar++;
                logger.info("Key : " + record.key() + " Value : " + record.value() + " Partition : " + record.partition() + " Offsets : " + record.offset() + " Topic : " + record.topic());

                if(numberOfMessageSoFar >= numberOfMessageToRead)
                {
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Closing Application");
    }

}
