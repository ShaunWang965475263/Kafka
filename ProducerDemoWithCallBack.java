package com.github.shaunwang965475263.Tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.font.TrueTypeFont;

import java.util.Properties;




public class ProducerDemoWithCallBack {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
        //Set properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"5");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create record

        //Send Asychronously
        for (int i = 1; i < 2; i++){
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World");
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if (e ==  null){
                            logger.info("Received new metadata. \n" +
                                    "Topics:" + recordMetadata.topic() +"\n"
                                    +"Parition:" + recordMetadata.partition() +"\n"+
                                    "Offset:" + recordMetadata.offset() + "\n" +
                                    "Timestamp:" + recordMetadata.timestamp());
                        }
                        else {
                            logger.error("Error while producing: ", e);

                    }
                }
            });
        }

        //send data
        producer.flush();

        producer.close();






    }
}
