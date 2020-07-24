package com.jw.jike_train;


/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Lesson 2 Stream Processing with Apache Flink
 *
 * @author xccui
 */
public class Training {
    private static List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    public static void main(String[] args) throws Exception {
//        System.out.println(declarative());
//        System.out.println(imperative1());
//        System.out.println(imperative3());
//        System.out.println(imperative2());
//        dataStream();
//        state();
//        processingTimeWindow();

        eventTimeWindow();
    }

    //----------------------------------------

    /**
     * 1. Naive
     */
    public static int imperative1() {
        List<Integer> tempList = new ArrayList<>(10);
        for (int v : data) {
            tempList.add(v * 2);
        }
        int result = 0;
        for (int v : tempList) {
            result += v;
        }
        return result;
    }

    /**
     * 2. In-place
     */
    public static int imperative2() {
        for (int i = 0; i < data.size(); ++i) {
            data.set(i, data.get(i) * 2);
        }
        int result = 0;
        for (int v : data) {
            result += v;
        }
        return result;
    }

    /**
     * 3. Optimized
     */
    public static int imperative3() {
        int result = 0;
        for (int v : data) {
            result += v * 2;
        }
        return result;
    }

    /**
     * 4. Functional
     */
    public static int declarative() {
        return data.stream().mapToInt(v -> v * 2).sum();
    }

    //----------------------------------------------------------------------------------

    /**
     * 5. Basic DataStream API
     */
    public static void dataStream() throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> source = e.addSource(
                new FromElementsFunction<>(Types.INT.createSerializer(e.getConfig()), data), Types.INT);
        DataStream<Integer> ds = source.map(v -> v * 2).keyBy(value -> 1).sum(0);
        ds.addSink(new PrintSinkFunction<>());
        System.out.println(e.getExecutionPlan());
        e.execute();
    }

    /**
     * 6. Sum with state.
     */
    public static void state() throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        e.fromCollection(data)
                .keyBy(v -> v % 2)
                .process(new KeyedProcessFunction<Integer, Integer, Integer>() {
                    private ValueState<Integer> sumState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Integer> sumDescriptor = new ValueStateDescriptor<>(
                                "Sum",
                                Integer.class);
                        sumState = getRuntimeContext().getState(sumDescriptor);
                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                        Integer oldSum = sumState.value();
                        int sum = oldSum == null ? 0 : oldSum;
                        sum += value;
                        sumState.update(sum);
                        out.collect(sum);
                    }
                }).print().setParallelism(2);

        e.execute();
    }

    /**
     * 7. Processing time tumbling window.
     */
    public static void processingTimeWindow() throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = e
                .addSource(new SourceFunction<Integer>() {
                    private volatile boolean stop = false;
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        int i = 0;
                        while (!stop && i < data.size()) {
                            ctx.collect(data.get(i++));
                            Thread.sleep(200);
                        }
                    }

                    @Override
                    public void cancel() {
                        stop = true;
                    }
                }).setParallelism(1);
        e.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); // optional for Processing time
        source.keyBy(v -> v % 2).process(new KeyedProcessFunction<Integer, Integer, Integer>() {
            private static final int WINDOW_SIZE = 400;
            private TreeMap<Long, Integer> windows;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                windows = new TreeMap<>();
            }

            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) {
                long currentTime = ctx.timerService().currentProcessingTime();
                long windowStart = currentTime / WINDOW_SIZE;
                // Update the window
                int sum = windows.getOrDefault(windowStart, 0);
                windows.put(windowStart, sum + value);

                // Fire old windows
                Map<Long, Integer> oldWindows = windows.headMap(windowStart, false);
                oldWindows.values();
                Iterator<Map.Entry<Long, Integer>> iterator = oldWindows.entrySet().iterator();
                while (iterator.hasNext()) {
                    out.collect(iterator.next().getValue());
                    iterator.remove();
                }
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println(windows);
            }
        }).print().setParallelism(2);
        e.execute();
    }











    /**
     * Homework: Event time tumbling window with state and timer.
     */
    public static void eventTimeWindow() throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        // A source with 500ms random delay.
        // 改写成使用Tuple2元组形式（数据，时间戳形式的）
        e.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Tuple2<Integer, Long>> source = e
                .addSource(new SourceFunction<Tuple2<Integer, Long>>() {
                    private volatile boolean stop = false;
                    private Random random = new Random();
                    @Override
                    public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
                        int i = 0;
                        while (!stop && i < data.size()) {
//                            ctx.collectWithTimestamp(
//                                    data.get(i++),
//                                    System.currentTimeMillis() - random.nextInt(500));
                            ctx.collect(new Tuple2<Integer, Long>(data.get(i++), System.currentTimeMillis() - random.nextInt(500)));
                            Thread.sleep(200);
                        }
                    }
                    @Override
                    public void cancel() {
                        stop = true;
                    }
                }).setParallelism(1);
        // TODO 1. Assign watermarks; 2. Use MapState to store windows; 3. Use timer to fire/cleanup.

        // 需要基于MapState进行水位线的设置
        // 还需要注意水位线的设置是有500ms的延迟,可以理解是500ms的乱序处理。
        source
                // 发送水位线操作。
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<Integer, Long>>() {

                    private Long maxTimeStamp = Long.valueOf(Integer.MIN_VALUE);

                    private Long maxOutOfOrder = 500L;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(maxTimeStamp - maxOutOfOrder);
                    }

                    @Override
                    public long extractTimestamp(Tuple2<Integer, Long> element, long previousElementTimestamp) {
                        Long timeStamp = element.f1;
                        maxTimeStamp = Math.max(maxTimeStamp, timeStamp);
                        System.out.println("------");
                        System.err.println(" timeStamp: " + timeStamp + " maxTimeStamp: " + maxTimeStamp + " watermark: " + (maxTimeStamp - maxOutOfOrder));
                        return maxTimeStamp;
                    }
                })
                // 按照Tuple2元组的f1时间戳来判定
                .keyBy(item -> item.f0 % 2)

                // 处理KeyedStream, 分别计算 奇数偶数的 window中的数据 计算窗口中的总和
                // 使用MapState来操作窗口的具体流程。
                .process(new KeyedProcessFunction<Integer, Tuple2<Integer, Long>, Integer>() {

                    private MapState<Long, Integer> mapState;
                    private ValueState<Long> preWindowEnd;
                    // 窗口的长度
                    private final Long WINDOW_SIZE = 1000L;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Integer>("mapState", Long.class, Integer.class));
                        preWindowEnd = getRuntimeContext().getState(new ValueStateDescriptor<Long>("preWindowEnd", Long.class));
                    }

                    @Override
                    public void processElement(Tuple2<Integer, Long> value, Context ctx, Collector<Integer> out) throws Exception {
                        long currentWatermark = ctx.timerService().currentWatermark();
                        int num = value.f0;
                        System.out.println(" 当前水位线: " + currentWatermark + " 当前数值：" + num + " 时间：" + value.f1);

                        System.out.println("=====");

                        long curWindowStart = currentWatermark / WINDOW_SIZE * WINDOW_SIZE;
                        // 说明没有窗口状态，将当前的watermark作为windowStart，并注册一个前闭后开的windowEnd作为Timer的触发事件
                        if (mapState.isEmpty()) {

                        } else {

                        }
                    }
                })
                .print();
        e.execute();
    }



    /*public static void eventTimeWindowDescription() throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        // A source with 500ms random delay.
        DataStreamSource<Integer> source = e
                .addSource(new SourceFunction<Integer>() {
                    private volatile boolean stop = false;
                    private Random random = new Random();
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        int i = 0;
                        while (!stop && i < data.size()) {
                            ctx.collectWithTimestamp(
                                    data.get(i++),
                                    System.currentTimeMillis() - random.nextInt(500));
                            Thread.sleep(200);
                        }

                    }
                    @Override
                    public void cancel() {
                        stop = true;
                    }
                }).setParallelism(1);
        // TODO 1. Assign watermarks; 2. Use MapState to store windows; 3. Use timer to fire/cleanup.

        e.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 需要基于MapState进行水位线的设置
        source
                // 按照奇数偶数进行聚合操作
                .keyBy(value -> value % 2)
                // key in out
                .process(new KeyedProcessFunction<Integer, Integer, Integer>() {

                    // 需要手动设置窗口长度，然后使用 除法/ 的方式来获得对应的windowStart并将其保存到MapState进行保存。
                    private static final int WINDOW_SIZE = 1000;
                    // key为windowStart，value为总和
                    private MapState<Long, Integer> mapState;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        MapStateDescriptor<Long, Integer> mapStateDescriptor = new MapStateDescriptor<>("sum", Long.class, Integer.class);
                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                        // 每次传来的数据中提取时间,数据源中的事件时间
                        Long timestamp = ctx.timestamp();

                        // 计算窗口的开始
                        Long windowStart = timestamp / WINDOW_SIZE;
                        System.out.println("windowStart: " + windowStart + " value: " + value + " timestamp: " + timestamp);

                        // 之前窗口有这个windowstart的数据
                        if (mapState.contains(windowStart)) {
                            int pre = mapState.get(windowStart);
                            // 更新数据

                            mapState.put(windowStart, pre + value);
                        } else {
                            mapState.put(windowStart, value);
                        }


                        // Fire比当前windowStart小的window
                        Iterator<Map.Entry<Long, Integer>> iterator = mapState.entries().iterator();
                        while (iterator.hasNext()) {
                            // 得到迭代器中的windowStart
                            Map.Entry<Long, Integer> entry = iterator.next();
                            Long preKey = entry.getKey();

                            if (windowStart > preKey) {
                                // 说明是已经到达下一个阶段的windowStart
                                System.out.println("fire window: " + preKey + " value: " + mapState.get(preKey));
                                out.collect(mapState.get(preKey));
                                iterator.remove();
                            }
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println(mapState.toString());
                    }
                })
        .print();



        e.execute();
    }*/

}

class Output {
    private Long windowEnd;
    private Integer type;
    private Integer sum;
}