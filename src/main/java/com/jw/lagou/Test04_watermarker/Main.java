package com.jw.lagou.Test04_watermarker;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple2<String, Long>> dataStream = env
                .socketTextStream("k8s2", 7777)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

                    private Long currentTimeStamp = 0L;
                    // 设置允许乱序的时间5s
                     private Long maxOutOfOrderness = 5000L;
                    // private Long maxOutOfOrderness = 0L;

                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimeStamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(String element, long previousElementTimestamp) {
                        String[] split = element.split(",");
                        Long timeStamp = Long.valueOf(split[1]);
                        currentTimeStamp = Math.max(currentTimeStamp, timeStamp);
                        System.err.println(element + " EventTime: " + timeStamp + " watermarker: " + (currentTimeStamp - maxOutOfOrderness));
                        return timeStamp;
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new Tuple2<>(split[0], Long.parseLong(split[1]));
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .timeWindow(Time.seconds(5))
                .minBy(1);

        dataStream.print();

        env.execute();
    }
}
