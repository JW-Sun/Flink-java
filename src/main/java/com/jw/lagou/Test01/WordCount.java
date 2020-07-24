package com.jw.lagou.Test01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromElements("hello spark", "hello flink");

        FlatMapOperator<String, Tuple2<String, Integer>> flatMap = data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] splitStr = value.split("\\W+");
                for (String s : splitStr) {
                    out.collect(new Tuple2<String, Integer>(s, 1));
                }
            }
        });
        

        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = flatMap.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.sum(1);

        sum.printToErr();
    }
}
