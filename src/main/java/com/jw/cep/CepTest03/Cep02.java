package com.jw.cep.CepTest03;

import com.jw.cep.CepTest03.pojo.SubEvent;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Cep02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
           1,valid,1,2019-10-18 01:00:30
           2,valid,1,2019-10-18 01:00:24
           3,valid,1,2019-10-18 01:00:28
           4,valid,200,2019-10-18 01:00:35
           5,valid,1,2019-10-18 01:00:45
        *
        * */

        DataStreamSource<String> source = env.socketTextStream("192.168.159.111", 7777);

        DataStream<SubEvent> input = source
                .map(new MapFunction<String, SubEvent>() {
                    @Override
                    public SubEvent map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new SubEvent(split[1], Integer.parseInt(split[2]), split[0], split[3]);
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SubEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(SubEvent element) {
                        String date = element.getDate();
                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        long res = 0L;

                        try {
                            Date parse = df.parse(date);
                            res = parse.getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                        return res;
                    }
                });

        // 定义CEP规则
        Pattern<SubEvent, SubEvent> pattern = Pattern
                .<SubEvent>begin("start")
                .where(new SimpleCondition<SubEvent>() {
                    @Override
                    public boolean filter(SubEvent value) throws Exception {
                        return "valid".equals(value.getType()) && value.getVolume() < 10;
                    }
                })
                .next("end")
                .where(new SimpleCondition<SubEvent>() {
                    @Override
                    public boolean filter(SubEvent value) throws Exception {
                        return "valid".equals(value.getType()) && value.getVolume() > 100;
                    }
                })
                .within(Time.seconds(10));

        PatternStream<SubEvent> patternStream = CEP.pattern(input, pattern);


        DataStream<String> result = patternStream.process(new PatternProcessFunction<SubEvent, String>() {
            @Override
            public void processMatch(Map<String, List<SubEvent>> match, Context ctx, Collector<String> out) throws Exception {
                SubEvent start = match.get("start").get(0);
                SubEvent end = match.get("end").get(0);
                StringBuilder bd = new StringBuilder();
                bd.append(start.getId() + " " + end.getId());
                out.collect(bd.toString());
            }
        });

        result.print();

        env.execute("cep2");

    }
}
