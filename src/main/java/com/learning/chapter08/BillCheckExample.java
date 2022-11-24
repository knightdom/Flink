package com.learning.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Created by Flink - BillCheckExample
 *
 * @Author: Edgar Fang
 * @Date: 2022/11/24 22:14
 */
public class BillCheckExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 来自app的支付日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L),
                Tuple3.of("order-3", "app", 3500L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }));

        // 来自第三方支付平台的支付日志
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdparyStream = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-3", "third-party", "success", 4000L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                                return element.f3;
                            }
                        }));


        // 检测同一支付单在两条流中是否匹配，不匹配就报警
        // 方式1 将两个KeyedStream类型进行connect
//        appStream.keyBy(data -> data.f0)
//                .connect(thirdparyStream.keyBy(data -> data.f0))

        // 方式2 将两个DataStream类型进行connect，再对ConnectedStreams类型进行keyBy分组
        appStream.connect(thirdparyStream)
                .keyBy(data -> data.f0, data -> data.f0)
                .process(new OrderMatchResult())    // 设置定时器，就是appStream等thirdparyStream多久
                .print();

        env.execute();
    }

    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {
        // 定义状态变量，用来保存已经到达的事件
        private ValueState<Tuple3<String, String, Long>> appEventStatus;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventStatus;

        @Override
        public void open(Configuration parameters) throws Exception {
            appEventStatus = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
            );

            thirdPartyEventStatus = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>("thirdparty-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
            );
        }

        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 来的时app event，看另一条流中事件是否来过
            if (thirdPartyEventStatus.value() != null) {
                out.collect("对账成功：" + value + " " + thirdPartyEventStatus.value());
                // 清空状态
                thirdPartyEventStatus.clear();
            } else {
                // 如果数据还没来 更新状态
                appEventStatus.update(value);
                // 注册一个5秒后的定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 来的时third party event，看另一条流中事件是否来过
            if (appEventStatus.value() != null) {
                out.collect("对账成功：" + value + " " + appEventStatus.value());
                // 清空状态
                appEventStatus.clear();
            } else {
                // 如果数据还没来 更新状态
                thirdPartyEventStatus.update(value);
                // 注册一个5秒后的定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f3);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，判断状态，如果某个状态不为空，说明另一条流中事件没来
            if (appEventStatus.value() != null) {
                out.collect("对账失败：" + appEventStatus.value() + " 第三方支付信息未到");
            }
            if (thirdPartyEventStatus.value() != null) {
                out.collect("对账失败：" + thirdPartyEventStatus.value() + " app信息未到");
            }
            // 清空两个状态中的信息
            appEventStatus.clear();
            thirdPartyEventStatus.clear();
        }
    }
}
