package com.jw.lagou.Test03_flinkSQL;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/*自定义的流数据源*/
public class MyStreamingSource implements SourceFunction<Item> {

    boolean b = true;

    @Override
    public void run(SourceContext<Item> sourceContext) throws Exception {
        while (b) {
            Item item = createItem();
            sourceContext.collect(item);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        b = false;
    }

    private Item createItem() {
        List<String> list = new ArrayList<>();
        list.add("food");
        list.add("shirt");
        list.add("electronic");

        Item item = new Item();
        item.setId(new Random().nextInt(10));
        item.setName(list.get(new Random().nextInt(3)));
        return item;
    }

}
