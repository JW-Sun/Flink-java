package com.jw.checkpoint.rocksDBStateBackend;

import com.jw.checkpoint.entity.Wc;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestCheckpoint {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StateBackend stateBackend = new RocksDBStateBackend("hdfs://192.168.159.102:9000/flink-checkpoint/RocksDBStateBackend");
        env.setStateBackend(stateBackend);

        // 两个CheckPoint的时间间隔，间隔越小就检查点越频繁，恢复时的数据就会相对变小，同时CheckPoint也会带来相应的IO资源消耗。
        env.enableCheckpointing(5000L);
        // 设置了Exactly Once的语义，就是要求了Barrier的对齐
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 两个Checkpoint之间最少要等待500ms。举例：CP1耗时1600ms，应该是还有400ms就要进行第二次的CP机制触发，
        // 但是时间间隔还需要等待100ms，这样就可以方式CP太过于频繁从而导致业务处理能力的下降。
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);
        // 当CP运行的时间超过10s的时候，这个CP就被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // 设置可以最大同时进行checkpoint机制的数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 当执行的任务被cancel的时候设置保留checkpoint，Checkpoint默认是整个作业在cancel的时候会被删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);



        DataStreamSource<String> source = env.socketTextStream("192.168.159.102", 7777);

        DataStream<Wc> sum = source
                .flatMap(new FlatMapFunction<String, Wc>() {
                    @Override
                    public void flatMap(String s, Collector<Wc> out) throws Exception {
                        String[] split = s.split(" ");
                        for (String spl : split) {
                            out.collect(new Wc(spl, 1));
                        }
                    }
                })
                .keyBy(new KeySelector<Wc, String>() {
                    @Override
                    public String getKey(Wc value) throws Exception {
                        return value.getWord();
                    }
                })
                .reduce(new ReduceFunction<Wc>() {
                    @Override
                    public Wc reduce(Wc value1, Wc value2) throws Exception {
                        int countSum = value1.getCount() + value2.getCount();
                        return new Wc(value1.getWord(), countSum);
                    }
                });

        sum.print();

        env.execute("WordCount");
    }


}
