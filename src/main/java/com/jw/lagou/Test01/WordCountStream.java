package com.jw.lagou.Test01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("192.168.159.102", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .sum(1);


        reduce.print().setParallelism(1);

        env.execute("wordcount");

    }
}

class WordCount2 {
    public String word;
    public Integer count;

    public WordCount2(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordCount2{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
