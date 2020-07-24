package com.jw.lagou.Test03_flinkSQL;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/* 把自定义的实时的数据进行分流，分成even和odd的流。进行join的条件是名称相同，把两个流的join结果输出 */
public class StreamingTest {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        DataStream<Item> source = bsEnv.addSource(new MyStreamingSource())
                .map(new MapFunction<Item, Item>() {
                    @Override
                    public Item map(Item item) throws Exception {
                        return item;
                    }
                });

        // 取出id为偶数的流
        DataStream<Item> evenSelect = source.split(new OutputSelector<Item>() {
            @Override
            public Iterable<String> select(Item value) {
                List<String> output = new ArrayList<>();
                if ((value.getId() & 1) == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        }).select("even");

        // 取出id为奇数的流
        DataStream<Item> oddSelect = source.split(new OutputSelector<Item>() {
            @Override
            public Iterable<String> select(Item value) {
                List<String> output = new ArrayList<>();
                if ((value.getId() & 1) == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        }).select("odd");

        // 将奇数偶数两个流生成对应的临时表
        bsTableEnv.createTemporaryView("evenTable", evenSelect, "id, name");
        bsTableEnv.createTemporaryView("oddTable", oddSelect, "id, name");

        Table queryTable = bsTableEnv.sqlQuery("select a.id, a.name, b.id, b.name " +
                "from evenTable a " +
                "join oddTable b on a.name = b.name");

        queryTable.printSchema();

        bsTableEnv.toRetractStream(queryTable, TypeInformation.of(new TypeHint<Tuple4<Integer, String, Integer, String>>(){}))
        .print();

        bsEnv.execute("streaming sql job");
    }
}
