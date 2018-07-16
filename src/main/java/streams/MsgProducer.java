/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package streams;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MsgProducer {

    // Declare a new producer
    public static KafkaProducer producer;

    public static void main(String[] args) throws Exception {

        // Set the default stream and topic to publish to.
        String fileName = "/mapr/demo.mapr.com/data/uber.csv";
        String topic = "/apps/uberstream:ubers";

        if (args.length == 2) {
            fileName = args[0];
            topic = args[1];
        } else {
            System.out.println("Using hard coded parameters unless you specify <file topic>   ");
        }

        System.out.println("Sending to topic " + topic);
        configureProducer();
        File f = new File(fileName);
        FileReader fr = new FileReader(f);
        BufferedReader reader = new BufferedReader(fr);
        String line = reader.readLine();
        Long temp;
        line = reader.readLine(); //skip header
        while (line != null) {

            /* Add each message to a record. A ProducerRecord object
             identifies the topic or specific partition to publish
             a message to. */
            line = line + "," + getReverseTimepstamp();
            ProducerRecord<String, String> rec = new ProducerRecord<>(topic, line);

            // Send the record to the producer client library.
            producer.send(rec);
            System.out.println("Sent message: " + line);
            Thread.sleep(60l);
            line = reader.readLine();

        }

        producer.close();
        System.out.println("All done.");

        System.exit(1);

    }

    /* Set the value for a configuration parameter.
     This configuration parameter specifies which class
     to use to serialize the value of each message.*/
    public static void configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    public static Long getReverseTimepstamp() {
        return java.lang.Long.MAX_VALUE - java.lang.System.currentTimeMillis();
    }

}
