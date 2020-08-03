package com.jw.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.aftermatch.SkipPastLastStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CepTest02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);

        // 数据源设置
        DataStream<Event> source = env.addSource(new SourceFunction<Event>() {
            private boolean running = true;

            @Override
            public void run(SourceContext<Event> ctx) throws Exception {
                String[] s = new String[]{"b", "b", "b", "c"};
                Map<String, Integer> map = new HashMap<>();
                int i = 0;
                int len = s.length;
                while (running) {
                    if (i == len) {
                        running = false;
                        continue;
                    }

                    map.put(s[i], map.getOrDefault(s[i], 0) + 1);
                    ctx.collect(new Event(s[i], map.get(s[i])));
                    i++;
//                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        // 匹配跳过策略
        // 1. 不丢弃任何结果
        AfterMatchSkipStrategy noSkip = AfterMatchSkipStrategy.noSkip();

        // 2. 丢弃以相同事件
        AfterMatchSkipStrategy skipToNext = AfterMatchSkipStrategy.skipToNext();

        // 3. 丢弃起始在这个匹配的开始和结束之间的所有匹配
        AfterMatchSkipStrategy skipPastLastEvent = AfterMatchSkipStrategy.skipPastLastEvent();

        // 4. 丢弃起始在这个匹配的开始和第一个出现的名称为PatternName事件之间的所有部分匹配。
        AfterMatchSkipStrategy skipToFirst = AfterMatchSkipStrategy.skipToFirst("start");

        // 5. 完成一个匹配之后，从指定的模式序列的最后一条记录开始作为寻找新匹配的第一条数据。
        AfterMatchSkipStrategy skipToLast = AfterMatchSkipStrategy.skipToLast("start");

        Pattern<Event, Event> eventPattern = Pattern
                .<Event>begin("start", skipToFirst)
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return "b".equals(value.getS());
                    }
                }).oneOrMore().greedy()
                .followedBy("end")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return "c".equals(value.getS());
                    }
                });

        PatternStream<Event> pattern = CEP.pattern(source, eventPattern);


        DataStream<String> process = pattern.process(new PatternProcessFunction<Event, String>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) throws Exception {
                StringBuilder bd = new StringBuilder();
                List<Event> start = match.get("start");
                List<Event> end = match.get("end");

                for (Event event : start) {
                    String s = event.getS();
                    int no = event.getNo();
                    bd.append(s + no + " ");
                }

                for (Event event : end) {
                    String s = event.getS();
                    int no = event.getNo();
                    bd.append(s + no + " ");
                }

                out.collect(bd.toString());
            }
        });

        process.print();

        env.execute("cep2");

    }
}
