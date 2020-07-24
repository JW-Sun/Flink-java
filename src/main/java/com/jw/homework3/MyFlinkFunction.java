package com.jw.homework3;

import com.alibaba.fastjson.JSON;
import com.jw.homework3.entity.KfkSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class MyFlinkFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.159.102:9092");
        properties.put("group.id", "testFlinkCanal");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");

        DataStream<String> input = env.addSource(new FlinkKafkaConsumer011<String>(
                "test_flink_canal",
                new SimpleStringSchema(),
                properties
        ));

        DataStream<KfkSource> kfkDataStream = input.map(new MapFunction<String, KfkSource>() {
            @Override
            public KfkSource map(String s) throws Exception {
                KfkSource kfkSource = JSON.parseObject(s, KfkSource.class);
                String baseInfo = kfkSource.getBase_info();
                String type = baseInfo.substring(baseInfo.lastIndexOf("-") + 1);
                kfkSource.setBase_info(type);
                return kfkSource;
            }
        });

        kfkDataStream.print();

        // 输出
        kfkDataStream.addSink(new MySqlSink());


        System.out.println("start working");
        env.execute();
    }
}
