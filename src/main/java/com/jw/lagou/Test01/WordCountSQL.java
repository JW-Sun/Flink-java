package com.jw.lagou.Test01;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;
import java.util.List;


public class WordCountSQL {

    public static class Wc {
        String word;
        Integer count;

        public Wc() {}

        public Wc(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " " + count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(env);

        String word = "hello world spark flink spark";

        String[] splitStr = word.split("\\W+");
        List<Wc> list = new ArrayList<>();

        for (String s : splitStr) {
            list.add(new Wc(s, 1));
        }

        DataSet<Wc> dataSet = env.fromCollection(list);

        Table table = batchTableEnvironment.fromDataSet(dataSet, "word,count");

        table.printSchema();

        batchTableEnvironment.createTemporaryView("wordcount", table);

        Table table2 = batchTableEnvironment.sqlQuery("select word as word, sum(`count`) as `count` " +
                "from wordcount " +
                "group by word");

        DataSet<Wc> wcDataSet = batchTableEnvironment.toDataSet(table2, Wc.class);
        wcDataSet.print();
    }
}
