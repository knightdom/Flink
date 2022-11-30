package com.learning.chapter09;

import com.learning.chapter05.ClickSource;
import com.learning.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Created by Flink - StateTest
 *
 * @Author: Edgar Fang
 * @Date: 2022/11/30 22:26
 */
public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        stream.keyBy(data -> data.user)
                .flatMap(new MyFlatMap())
                .print();

        env.execute();
    }

    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {
        // 定义状态
        ValueState<Event> myValueState;
        ListState<Event> myListState;
        MapState<String, Long> myMapState;
        ReducingState<Event> myReducingState;
        AggregatingState<Event, String> myAggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            myValueState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("my-state", Event.class));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("my-list", Event.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map", String.class, Long.class));

            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("my-reduce",
                    new ReduceFunction<Event>() {
                        @Override
                        public Event reduce(Event value1, Event value2) throws Exception {
                            return new Event(value1.user, value1.url, value2.timestamp);
                        }
                    }
                    , Event.class));
            myAggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("my-agg",
                    new AggregateFunction<Event, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Event value, Long accumulator) {
                            return accumulator + 1;
                        }

                        @Override
                        public String getResult(Long accumulator) {
                            return "count: " + accumulator;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return a + b;
                        }
                    }
                    , Long.class));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            // 访问和更新状态
//            System.out.println(myValueState.value());
            myValueState.update(value);
            System.out.println("my value: " + myValueState.value());

            myListState.add(value);
            System.out.println("my list: " + myListState.get());

            myMapState.put(value.user, myMapState.get(value.user) == null ? 1 : myMapState.get(value.user) + 1);
            System.out.println("my map: " + value.user + " " + myMapState.get(value.user));

            myReducingState.add(value);
            System.out.println("my reducing: " + myReducingState.get());

            myAggregatingState.add(value);
            System.out.println("my aggregating: " + myAggregatingState.get());
        }
    }
}
