package com.benson.nightingale;

import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.BaseKey;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.OffsetKey;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OffsetConsumer extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(OffsetConsumer.class);

    public static final String TOPIC = "__consumer_offsets";
    public static final String KAFKA_SERVER_URL = "pdstore3.safe.lycc.qihoo.net";
    public static final int KAFKA_SERVER_PORT = 9092;

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final String topic;

    public OffsetConsumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "benson");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        //props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        //props.put("sasl.mechanism", "SCRAM-SHA-256");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"alleria\" password=\"jlfkajkj\";");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(this.topic));
            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<byte[], byte[]> record : records) {

                    //LOG.info("receive message, key : {}, value : {}, offset : {}", record.key(), record.value(), record.offset());

                    if (record.key() == null) {
                        continue;
                    }
                    BaseKey baseKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key()));
                    byte[] value = record.value();
                    if (value == null) {
                        continue;
                    }
                    if (baseKey instanceof OffsetKey) {
                        OffsetKey newKey = (OffsetKey) baseKey;
                        OffsetAndMetadata offsetMeta = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value));
                        TopicPartition tp = newKey.key().topicPartition();
                        String group = newKey.key().group();

                        LOG.info("commit timestamp : {}", offsetMeta.commitTimestamp());
                        LOG.info("commit group : {}", group);
                        LOG.info("commit topic : {}", tp.topic());
                        LOG.info("commit partition : {}", tp.partition());
                        LOG.info("commit offset : {}", offsetMeta.offset());
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        OffsetConsumer consumerThread = new OffsetConsumer(TOPIC);
        consumerThread.start();
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}