package com.jw.homework2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;

public class Main {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Top10Product> top10ProductDataStreamSource = env.addSource(new MySourceFromMySql());

        String url = "hdfs://192.168.159.102:9000/homework/flink";
        BucketingSink<Top10Product> bs = new BucketingSink<>(url);
        bs.setInactiveBucketCheckInterval(1L);
        bs.setInactiveBucketThreshold(1L);

        top10ProductDataStreamSource.map(new MapFunction<Top10Product, Object>() {
            @Override
            public Object map(Top10Product value) throws Exception {
                return null;
            }
        });

        top10ProductDataStreamSource.keyBy(new KeySelector<Top10Product, Object>() {
            @Override
            public Object getKey(Top10Product value) throws Exception {
                return null;
            }
        });


        top10ProductDataStreamSource.addSink(bs);

        env.execute("top10");
    }
}
