package com.atguigu.gmall2019.canal.util;/*
  需求：
  思路：
   技术点：
**/

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MykafkaSender {
    public static KafkaProducer<String, String> kafkaProducer=null;


    public  static KafkaProducer<String, String> createKafkaProducer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop11:9092,hadoop12:9092,hadoop13:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);

        }catch (Exception e){
            e.printStackTrace();
        }
        return producer;
    }

    public static  void send(String topic,String msg){
        if(kafkaProducer==null){
            kafkaProducer=createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String, String>(topic,msg));
    }

}
