package com.learning.chapter06;

import com.learning.chapter05.ClickSource;
import com.learning.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * Created by Flink - WindowAggregateTest
 *
 * @Author: Edgar Fang
 * @Date: 2022/11/15 22:57
 */
public class WindowAggregateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 获取数据 设置水位线
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // AggregateFunction 聚合函数
        stream.keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Event, Tuple3<String, Long, Integer>, Tuple2<String, String>>() {
                    @Override
                    // 创建累加器
                    public Tuple3<String, Long, Integer> createAccumulator() {
                        return Tuple3.of("", 0L, 0);
                    }

                    @Override
                    // 每次执行时调用该函数，用于修改累加器状态
                    public Tuple3<String, Long, Integer> add(Event value, Tuple3<String, Long, Integer> accumulator) {
                        return Tuple3.of(value.user, accumulator.f1 + value.timestamp, accumulator.f2 + 1);
                    }

                    @Override
                    // 通过累加器获取最终的结果
                    public Tuple2<String, String> getResult(Tuple3<String, Long, Integer> accumulator) {
                        Timestamp timestamp = new Timestamp(accumulator.f1 / accumulator.f2);
                        return Tuple2.of(accumulator.f0, timestamp.toString());
                    }

                    @Override
                    // 将多个累加器合并，一般用于session window
                    public Tuple3<String, Long, Integer> merge(Tuple3<String, Long, Integer> a, Tuple3<String, Long, Integer> b) {
                        return Tuple3.of(a.f0 + " " + b.f0, a.f1 + b.f1, a.f2 + b.f2);
                    }
                })
                .print();

        env.execute();
    }
}
