package com.jw.checkpoint.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class KafkaUtil {

    public static Properties getConsumerProp() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "192.168.159.102:9092, 192.168.159.103:9092, 192.168.159.104:9092");
        prop.put("group.id", "test02_group");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("auto.offset.reset", "latest");

        return prop;
    }

    public FlinkKafkaConsumer011<String> getFlinkKafkaConsumer(String topic) {
        Properties prop = getConsumerProp();

        FlinkKafkaConsumer011<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer011<>(
                topic,
                new SimpleStringSchema(),
                prop
        );
        return stringFlinkKafkaConsumer;
    }

    private static Properties getProducerProp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.159.102:9092, 192.168.159.103:9092, 192.168.159.104:9092");
        props.put("acks", "all"); // 发送所有ISR
        props.put("retries", 2); // 重试次数
        props.put("batch.size", 16384); // 批量发送大小
        props.put("buffer.memory", 33554432); // 缓存大小，根据本机内存大小配置
        props.put("linger.ms", 1000); // 发送频率，满足任务一个条件发送
        props.put("client.id", "producer-syn-1"); // 发送端id,便于统计
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public FlinkKafkaProducer011<String> getFlinkKafkaProducer(String topic) {
        FlinkKafkaProducer011<String> stringFlinkKafkaProducer011 = new FlinkKafkaProducer011<>(
                "192.168.159.102:9092, 192.168.159.103:9092, 192.168.159.104:9092",
                topic,
                new SimpleStringSchema()
        );
        return stringFlinkKafkaProducer011;
    }


}
