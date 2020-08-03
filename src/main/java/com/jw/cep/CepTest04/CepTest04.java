package com.jw.cep.CepTest04;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CepTest04 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    }
}
