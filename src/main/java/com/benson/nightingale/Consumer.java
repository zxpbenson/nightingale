package com.benson.nightingale;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer extends Thread {

//    public static final String TOPIC = "alleria_output_middle_result_test";
//    public static final String KAFKA_SERVER_URL = "10.160.228.42";
//    public static final int KAFKA_SERVER_PORT = 10011;
    public static final String TOPIC = "powershell_log";
    public static final String KAFKA_SERVER_URL = "10.173.194.217";
    public static final int KAFKA_SERVER_PORT = 39092;

    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;

    public Consumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "alleria.driver.test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        //props.put("sasl.mechanism", "SCRAM-SHA-256");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"alleria\" password=\"jlfkajkj\";");


        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    public void run() {
        try{
            consumer.subscribe(Collections.singletonList(this.topic));
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        Consumer consumerThread = new Consumer(TOPIC);
        consumerThread.start();
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}