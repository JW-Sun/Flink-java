package com.jw.cep.CepTest03;

import com.jw.cep.CepTest03.pojo.Alert;
import com.jw.cep.CepTest03.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.util.List;
import java.util.Map;
import java.util.Properties;


public class Cep01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // DataStream<String> source = env.readTextFile("E:\\JavaIDEAProject\\ProjectGroup\\Flink-java\\src\\main\\java\\com\\jw\\cep\\CepTest03\\1.txt");

        Properties properties = new Properties();
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), properties));

        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println(value);
                return value;
            }
        });

        DataStream<Event> map = source.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                String[] split = value.split(",");
                return new Event(split[1], Integer.parseInt(split[2]), split[0]);
            }
        });

        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        // System.out.println(value + " from start ");
                        return value.getType().equals("valid") && value.getVolume() < 10;
                    }
                })
                .next("end")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        // System.out.println(value + " from end ");
                        return value.getType().equals("valid") && value.getVolume() > 100;
                    }
                });

        PatternStream<Event> patternStream = CEP.pattern(map, pattern);

        SingleOutputStreamOperator<String> process = patternStream.process(new PatternProcessFunction<Event, String>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) throws Exception {
//                System.out.println(pattern);
                List<Event> start = match.get("start");
                List<Event> end = match.get("end");

                StringBuilder bd = new StringBuilder();

                for (Event event : start) {
                    bd.append(event.getId() + " ");
                }
                for (Event event : end) {
                    bd.append(event.getId() + " ");
                }

                out.collect(bd.toString());

            }
        });

        process.print();

        env.execute();
    }
}
