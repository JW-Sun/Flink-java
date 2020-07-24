package com.jw.lagou.Test05_state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestState {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<Integer, Integer>> source = env.fromElements(
                Tuple2.of(1, 3),
                Tuple2.of(1, 5),
                Tuple2.of(1, 7),
                Tuple2.of(1, 5),
                Tuple2.of(1, 2)
        );


        KeyedStream<Tuple2<Integer, Integer>, Integer> keyedStream = source.keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, Integer> value) throws Exception {
                return value.f0;
            }
        });
        
        keyedStream.flatMap(new RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

            private ValueState<Tuple2<Integer, Integer>> sum;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor = new ValueStateDescriptor<>(
                        "sum",
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                        }));

                StateTtlConfig stateTtlConfig = StateTtlConfig
                        .newBuilder(Time.seconds(10))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();

                descriptor.enableTimeToLive(stateTtlConfig);

                sum = getRuntimeContext().getState(descriptor);


            }

            @Override
            public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                Tuple2<Integer, Integer> cur;
                if (sum.value() == null) {
                    cur = Tuple2.of(0, 0);
                } else {
                    cur = sum.value();
                }

                cur.f0++;
                cur.f1 += value.f1;
                sum.update(cur);
                if (cur.f0 >= 2) {
                    out.collect(new Tuple2<>(value.f0, cur.f1 / cur.f0));
                    sum.clear();
                }

            }
        })
        .printToErr();

        env.execute();
    }
}
