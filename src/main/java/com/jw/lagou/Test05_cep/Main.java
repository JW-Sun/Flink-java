package com.jw.lagou.Test05_cep;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, String, Long>> source = env.fromElements(
                //浏览记录
                Tuple3.of("Marry", "外套", 1L),
                Tuple3.of("Marry", "帽子", 1L),
                Tuple3.of("Marry", "帽子", 2L),
                Tuple3.of("Marry", "帽子", 3L),
                Tuple3.of("Ming", "衣服", 1L),
                Tuple3.of("Marry", "鞋子", 1L),
                Tuple3.of("Marry", "鞋子", 2L),
                Tuple3.of("LiLei", "帽子", 1L),
                Tuple3.of("LiLei", "帽子", 2L),
                Tuple3.of("LiLei", "帽子", 3L)
        );

        // 寻找连续搜索帽子的用户
        Pattern<Tuple3<String, String, Long>, Tuple3<String, String, Long>> pattern = Pattern
                .<Tuple3<String, String, Long>>begin("start")
                .where(new IterativeCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> stringStringLongTuple3, Context<Tuple3<String, String, Long>> context) throws Exception {
                        return "帽子".equals(stringStringLongTuple3.f1);
                    }
                })
                .next("middle")
                .where(new IterativeCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> stringStringLongTuple3, Context<Tuple3<String, String, Long>> context) throws Exception {
                        return "帽子".equals(stringStringLongTuple3.f1);
                    }
                });

        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedStream = source.keyBy(0);

        PatternStream<Tuple3<String, String, Long>> patternStream = CEP.pattern(keyedStream, pattern);

        SingleOutputStreamOperator<String> matchStream = patternStream.select(new PatternSelectFunction<Tuple3<String, String, Long>, String>() {
            @Override
            public String select(Map<String, List<Tuple3<String, String, Long>>> map) throws Exception {
                List<Tuple3<String, String, Long>> middle = map.get("middle");
                return middle.get(0).f0 + " : " + middle.get(0).f2 + " : 连续搜索的两次帽子！";
            }
        });

        matchStream.print();
        env.execute();

    }
}
