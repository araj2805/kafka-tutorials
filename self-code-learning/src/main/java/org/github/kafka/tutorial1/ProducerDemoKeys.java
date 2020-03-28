package org.github.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServer="127.0.0.1:9092";
        String topic = "first_topic";


        //create producer propeties
        Properties properties =new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

            // create new producer
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for(int i =0;i<20;i++)
        {
            String value = "Hello i'm from java "+String.valueOf(i);
            String key = "_key_"+String.valueOf(i);

            // create producer records
            ProducerRecord<String,String> record =new ProducerRecord<String, String>(topic,key,value);

            logger.info("Key : "+key);

            // Sending data to producer
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null)
                    {
                        logger.info("Recieved new metadata. \n"+
                                "Topic : "+metadata.topic()+ "\n"+
                                "Partitions : "+metadata.partition()+"\n"+
                                "Offset : "+metadata.offset()+"\n"+
                                "TimeStamps : "+metadata.timestamp());
                    }
                    else {
                            logger.error("Error while producing "+ exception);
                    }
                }
            }).get();

        }

        // flush data
        producer.flush();

        //close kafka
        producer.close();
    }
}
