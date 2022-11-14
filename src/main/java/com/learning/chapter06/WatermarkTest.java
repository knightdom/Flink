package com.learning.chapter06;

import com.learning.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Created by Flink - WatermarkTest
 *
 * @Author: Edgar Fang
 * @Date: 2022/11/8 23:46
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);  // 默认的设置水位线的周期是200ms

        // 获取数据
        SingleOutputStreamOperator<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alic", "./prod", 3000L),
                new Event("Bob", "./prod?id=2", 3500L),
                new Event("Alice", "./cart", 3600L),
                new Event("Bob", "./home", 3800L),
                new Event("Bob", "./cart", 4000L),
                new Event("Mary", "./cart", 3600L)
        )
                // 有序流的watermark生成 （一般用于测试）
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                            // 从数据中获取时间戳
//                            @Override
//                            public long extractTimestamp(Event element, long recordTimestamp) {
//                                return element.timestamp;
//                            }
//                        }));
                // 无序留的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        stream.keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))     // 滚动事件时间窗口
//                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))     // 滑动事件时间窗口
//                .window(EventTimeSessionWindows.withGap(Time.minutes(2)))   // 事件时间会话窗口
//                .window(ProcessingTimeSessionWindows.withGap(Time.minutes(2)))  // 处理时间会话窗口
//                .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(5)))    // 滑动处理事件窗口
//                .window(TumblingProcessingTimeWindows.of(Time.hours(1)))    // 滚动处理事件窗口
//                .countWindow(10)     // 滚动计数窗口
//                .countWindow(10, 2)         // 滑动计数窗口
    }
}
