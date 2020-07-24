package com.jw.lagou.Test02;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

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
        Item item = new Item();
        item.setId(new Random().nextInt(10));
        item.setName(UUID.randomUUID().toString());
        return item;
    }
}
