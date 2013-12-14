package org.ossandme;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;

public class MyProducer {

    public static void main(String[] args) {

        // producer configuration
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        // create producer
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        for (long cnt = 0; cnt < 100; cnt++) {
            long time = new Date().getTime(); // get current time
            String msg = String.valueOf(time);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("ossandme", "0", msg);
            producer.send(data); // dispatch message to broker
        }

        System.out.println("Messages published!!");

        producer.close();
    }

}