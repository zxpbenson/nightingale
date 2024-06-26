package com.benson.nightingale;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ProducerPress {
    public static void main(String[] args) {

        int qps = Integer.valueOf(args[0]);
        int producerSize = Integer.valueOf(args[1]);

        String[] dataArr = new String[0];
        try {
            dataArr = loadData(args[2]);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        BlockingQueue<String> bq = new ArrayBlockingQueue<String>(qps);
        KafkaProducerThread producerThread = new KafkaProducerThread(bq, producerSize);
        producerThread.start();

//        // 尝试所有数据
//        for (String msg : dataArr) {
//            try {
//                bq.put(msg);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//        if (true) {
//            while (!bq.isEmpty()) {
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//            producerThread.interrupt();
//            return;
//        }

        // 压测
        while (true) {
            try {
                MsgGeneratorThread msgGeneratorThread = new MsgGeneratorThread(bq, qps, dataArr);
                msgGeneratorThread.start();
                Thread.sleep(1000);
                long start = System.currentTimeMillis();
                msgGeneratorThread.join();
                long end = System.currentTimeMillis();
                System.out.println("send time delay : " + (end - start));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private static String[] loadData(String path) throws Exception {
        String line = null;
        ArrayList<String> list = new ArrayList<String>();
        try (
                FileReader fr = new FileReader(path);
                BufferedReader br = new BufferedReader(fr);
        ) {
            while ((line = br.readLine()) != null) {
                if (line != null) {
                    list.add(line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list.toArray(new String[]{});
    }
}

class MsgGeneratorThread extends Thread {

    private ThreadSafeRandom random = new ThreadSafeRandom();
    private BlockingQueue<String> bq;
    private int qps;
    private String[] dataArr;
    private int len;

    public MsgGeneratorThread(BlockingQueue<String> bq, int qps, String[] dataArr) {
        this.bq = bq;
        this.qps = qps;
        this.dataArr = dataArr;
        this.len = dataArr.length;
    }

    @Override
    public void run() {
        int counter = 0;
        while (counter++ < qps) {
            try {
                sendMsg();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void sendMsg() throws InterruptedException {
        int index = random.nextInt(len);
        bq.put(this.dataArr[index]);
    }
}

class KafkaProducerThread extends Thread {
    // nzstore2
//    public static final String TOPIC = "alleria_input_event_test";
//    public static final String KAFKA_SERVER_URL = "10.160.228.42";
//    public static final int KAFKA_SERVER_PORT = 10011;

    // local dev
//    public static final String TOPIC = "alleria_input_event_dev";
//    public static final String KAFKA_SERVER_URL = "127.0.0.1";
//    public static final int KAFKA_SERVER_PORT = 9092;

    //nzstore16
    public static final String TOPIC = "alleria_input_event";
    public static final String KAFKA_SERVER_URL = "127.0.0.1";
    public static final int KAFKA_SERVER_PORT = 9092;

    private BlockingQueue<String> bq;
    private int producerSize;
    private List<KafkaProducer<Integer, String>> producerList;
    private int messageNo = 0;
    private int producerNo = 0;

    public KafkaProducerThread(BlockingQueue<String> bq, int producerSize) {
        this.bq = bq;
        this.producerSize = producerSize;
        producerList = new ArrayList<KafkaProducer<Integer, String>>();
        for(int i = 0; i < producerSize; i++){
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer_" + i);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            //props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            //props.put("sasl.mechanism", "SCRAM-SHA-256");
            //props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"alleria\" password=\"jlfkajkj\";");
            producerList.add(new KafkaProducer<Integer, String>(props));
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                sendMsg();
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
        for(KafkaProducer<Integer, String> producer : producerList){
            try {
                producer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void sendMsg() throws InterruptedException {
        String message = bq.take();
        try{
            producerList.get(producerNo++).send(new ProducerRecord<>(TOPIC, messageNo++, message));
            //System.out.println(message);
        }finally{
            if(producerNo >= producerSize){
                producerNo = 0;
            }
        }
    }
}

class ThreadSafeRandom {

    private final ThreadLocal<Random> randomThreadLocal = new ThreadLocal<Random>() {
        protected Random initialValue() {
            return new Random();
        }
    };

    public int nextInt(int bound) {
        return randomThreadLocal.get().nextInt(bound);
    }

    public int nextInt() {
        return randomThreadLocal.get().nextInt();
    }

    public long nextLong() {
        return randomThreadLocal.get().nextLong();
    }

    public boolean nextBoolean() {
        return randomThreadLocal.get().nextBoolean();
    }

    public double nextDouble() {
        return randomThreadLocal.get().nextDouble();
    }

    public float nextFloat() {
        return randomThreadLocal.get().nextFloat();
    }
}