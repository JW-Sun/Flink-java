package com.jw.streamingwithflink.chapter1;

import com.jw.streamingwithflink.util.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class AverageSensorReadings {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> source = env
                .addSource(new RichParallelSourceFunction<SensorReading>() {

                    private boolean running = true;

                    @Override
                    public void run(SourceContext<SensorReading> ctx) throws Exception {
                        Random random = new Random();

                        // 查找当前并行任务的索引index
                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

                        List<Tuple2<String, Double>> list = new ArrayList<>();

                        for (int i = 1; i <= 10; i++) {
                            list.add(new Tuple2<String, Double>("sensor_" + (indexOfThisSubtask * 10 + i), 65 + (random.nextGaussian() * 20)));
                        }

                        while (running) {
                            for (Tuple2<String, Double> item : list) {
                                item.setFields(item.f0, item.f1 + (random.nextGaussian() * 0.5));
                            }

                            long timeInMillis = Calendar.getInstance().getTimeInMillis();

                            for (Tuple2<String, Double> item : list) {
                                ctx.collect(new SensorReading(item.f0, timeInMillis, item.f1));
                            }

                            Thread.sleep(500);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimeStamp();
                    }
                });


        source
                .map(new MapFunction<SensorReading, SensorReading>() {
                    @Override
                    public SensorReading map(SensorReading value) throws Exception {
                        return new SensorReading(value.getId(), value.getTimeStamp(), (value.getTemperature() - 32) * (5.0 / 9));
                    }
                })
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading value) throws Exception {
                        return value.getId();
                    }
                })

                .timeWindow(Time.seconds(1))

                .apply(new WindowFunction<SensorReading, SensorReading, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) throws Exception {
                        int count = 0;
                        double sum = 0D;

                        Iterator<SensorReading> iterator = input.iterator();
                        while (iterator.hasNext()) {
                            SensorReading se = iterator.next();
                            Double temperature = se.getTemperature();
                            count++;
                            sum += temperature;
                        }

                        double avg = sum / count;
                        out.collect(new SensorReading(s, window.getEnd(), avg));
                    }
                })

                .print();

        env.execute("1");

    }
}
