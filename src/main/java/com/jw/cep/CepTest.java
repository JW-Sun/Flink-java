package com.jw.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Event {
    private String s;
    private int no;

    public Event() {
    }

    public Event(String s, int no) {
        this.s = s;
        this.no = no;
    }

    @Override
    public String toString() {
        return "Event{" +
                "s='" + s + '\'' +
                ", no=" + no +
                '}';
    }

    public String getS() {
        return s;
    }

    public void setS(String s) {
        this.s = s;
    }

    public int getNo() {
        return no;
    }

    public void setNo(int no) {
        this.no = no;
    }
}

public class CepTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);

        // 数据源设置
        DataStream<Event> source = env.addSource(new SourceFunction<Event>() {
            private boolean running = true;

            @Override
            public void run(SourceContext<Event> ctx) throws Exception {
                String[] s = new String[]{"c", "d", "a", "a", "a", "d", "a", "b", "c", "a", "a", "b"};
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

        Pattern<Event, Event> eventPattern = Pattern
                .<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return "c".equals(value.getS());
                    }
                })
                .followedBy("middle")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return "a".equals(value.getS());
                    }
                }).oneOrMore()
//                .allowCombinations()
                // .consecutive()
                .followedBy("end")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return "b".equals(value.getS());
                    }
                });

        PatternStream<Event> pattern = CEP.pattern(source, eventPattern);


        DataStream<String> process = pattern.process(new PatternProcessFunction<Event, String>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) throws Exception {
                StringBuilder bd = new StringBuilder();
                List<Event> start = match.get("start");
                List<Event> middle = match.get("middle");
                List<Event> end = match.get("end");
                bd.append(start.get(0).getS() + start.get(0).getNo());
                bd.append(" ");
                for (Event event : middle) {
                    String s = event.getS();
                    int no = event.getNo();
                    bd.append(s + no + " ");
                }

                bd.append(end.get(0).getS() + end.get(0).getNo());

                out.collect(bd.toString());
            }
        });

        process.print();

        env.execute();

    }
}
