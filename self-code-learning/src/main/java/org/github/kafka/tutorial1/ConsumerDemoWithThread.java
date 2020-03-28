package org.github.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();

    }

    private void run()
    {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating consumer Thread");
        Runnable myConsumerThread = new ConsumerThreads(bootstrapServer,groupId,topic,latch);

        Thread myThread = new Thread(myConsumerThread);
        myThread.run();

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted : "+e);
        }finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThreads implements Runnable {

        Logger logger = LoggerFactory.getLogger(ConsumerThreads.class.getName());

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThreads(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {

            // Setting Property
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.latch = latch;
            //create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            //Subscribe the consumer to our topics
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {

            //poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key : " + record.key() + " Value : " + record.value() + " Partition : " + record.partition() + " Offsets : " + record.offset() + " Topic : " + record.topic());
                    }
                }
            } catch (WakeupException e) {
                logger.error("WakeUp signal is obtained!!!");
            } finally {
                consumer.close();
                // tell main code that we're done with consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // wakeup is special method that interrupt the consumer.poll method and through wakeup exception
            consumer.wakeup();
        }
    }

}
