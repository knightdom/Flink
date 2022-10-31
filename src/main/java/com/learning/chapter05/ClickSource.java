package com.learning.chapter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * Created by Flink - ClickSource
 *
 * @Author: Edgar Fang
 * @Date: 2022/10/31 21:33
 */
public class ClickSource implements SourceFunction<Event> {
    // 声明一个标识位
    private boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        // 随机生成数据
        Random random = new Random();
        // 定义字段选取的数据集
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=100", "./prod?id=10"};

        // 循环生成数组
        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            long timestamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user, url, timestamp));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
