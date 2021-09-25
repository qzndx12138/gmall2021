package com.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author 刘帅
 * @create 2021-09-22 20:44
 */


public class MyKafkaUtil {

    public static String brokers = "CJhadoop102:9092,CJhadoop103:9092,CJhadoop104:9092";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(brokers, topic, new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaProducer<T>("DWD_DEFAULT_TOPIC",kafkaSerializationSchema,properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);   //配置可以消费消息的消费者服务器
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);  //配置消息者的groupid

        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);

    }
}
