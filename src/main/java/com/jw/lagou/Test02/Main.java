package com.jw.lagou.Test02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Item> input = env.addSource(new MyStreamingSource());

        DataStream<String> map = input.map(new MapFunction<Item, String>() {

            @Override
            public String map(Item item) throws Exception {
                return item.getName();
            }
        });

        DataStream<Character> flatMap = input.flatMap(new FlatMapFunction<Item, Character>() {
            @Override
            public void flatMap(Item value, Collector<Character> out) throws Exception {
                for (char c : value.getName().toCharArray()) {
                    out.collect(c);
                }
            }
        });

        // map.print();

        // flatMap.print();

        

        env.execute();
    }
}
