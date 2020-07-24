package com.jw.homework3;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class MyKafkaProducer {

    private static KafkaProducer<String, String> kafkaProducer;

    public static KafkaProducer<String, String> createProducer() {
        Properties properties = new Properties();
        // 必选三项 Kafka服务端的主机名和端口号 key序列化 value序列化
        properties.put("bootstrap.servers", "192.168.159.102:9092,192.168.159.103:9092,192.168.159.104:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 集群leader收到消息，并将消息写入到副本后进行返回ack信息。如果生产者数据没有到达Leader节点或者没有写入
        // 到leader节点的副本节点中，那么就会重新发送消息。但是还有一种情况，在其他follower节点在从leader节点获得数据前
        // leader节点挂了，就会导致其他follower节点没有收到消息但生产者会误认为数据发送完成，造成数据缺失。ack = 1是默认配置
        properties.put("acks", "1");
        // 生产者重复次数，消息从个生产者发出到送到broker这个阶段会产生临时性的异常（网络抖动、leader副本选举异常），通过
        // 增加重复次数来进行适当的恢复工作。
        properties.put("retries", 3);
        properties.put("batch.size", 10);
        // 这个参数用来指定生产者发送ProducerBatch之前等待更多的消息（ProducerRecord）假如ProducerBatch的时间，默认值为0。
        // ProducerBatch在被填满或者时间超过linger.ms值时发送出去。增大这个参数的值回增加消息的延迟（消费端接收延迟），但能够提升一定的吞吐量。
        properties.put("linger.ms", 10);
        // 生产者客户端RecordAccumulator缓存大小
        properties.put("buffer.memory", 256);

        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return producer;
    }

    public static void send(String topic, String msg) {
        if (kafkaProducer == null) {
            kafkaProducer = createProducer();
        }
        System.out.println("准备发送");
        kafkaProducer.send(new ProducerRecord<String, String>(topic, msg), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    System.out.println("收到数据");
                    System.out.println(recordMetadata.partition() + "---" + recordMetadata.offset());
                    System.out.println();
                } else {
                    e.printStackTrace();
                }
            }
        });
//        kafkaProducer.send(new ProducerRecord<String, String>(topic, msg));
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            String msg = String.valueOf(System.currentTimeMillis());
            send("test_flink_canal", msg);
            Thread.sleep(1000);
        }
    }

}
