package com.learning.chapter06;

import com.learning.chapter05.ClickSource;
import com.learning.chapter05.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * Created by Flink - EventTimeTimerTest
 *
 * @author Edgar Fang
 * @date 2022/11/18 16:04
 */
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 事件时间定时器
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        long currTs = ctx.timestamp();  // 当前时间
                        out.collect(ctx.getCurrentKey() + "数据到达，到达时间：" + new Timestamp(currTs) + " 当前Watermark：" + ctx.timerService().currentWatermark());  // 当前数据进入开始处理的时候，watermark依然还是上一条数据的事件时间，等当前数据处理后，watermark才会变成这条数据的事件时间。

                        // 注册一个10秒后的定时器
                        ctx.timerService().registerEventTimeTimer(currTs + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "定时器触发，触发时间：" + new Timestamp(timestamp));
                    }
                }).print();

        env.execute();
    }
}
