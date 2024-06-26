package com.benson.nightingale;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConsumerMonitor {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "alleria3.safe.lycc.qihoo.net:27002");
        AdminClient ac = AdminClient.create(prop);

        listGroups(ac);
        listTopics(ac);


        ac.close();

    }

    public static void listTopics(AdminClient ac) throws ExecutionException, InterruptedException {
        ListTopicsResult ltr = ac.listTopics();
        KafkaFuture<Collection<TopicListing>> kf = ltr.listings();
        Collection<TopicListing> tls = kf.get();
        for(TopicListing tl : tls){
            System.out.println(tl);
        }

    }

    public static void listGroups(AdminClient ac) throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult lcgr = ac.listConsumerGroups();
        KafkaFuture<Collection<ConsumerGroupListing>> kf = lcgr.all();
        Collection<ConsumerGroupListing> cgls = kf.get();
        for (ConsumerGroupListing cgl : cgls) {
            System.out.println(cgl);
        }
    }
}
