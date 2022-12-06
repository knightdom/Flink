package com.learning.chapter09;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Created by Flink - BehaviorPatternDetectExample
 *
 * @Author: Edgar Fang
 * @Date: 2022/12/6 21:45
 */
public class BehaviorPatternDetectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 用户的行为数据流
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "order")
        );

        // 行为模式流，基于它构建广播流
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "order")
        );

        // 定义广播状态描述器
        MapStateDescriptor<Void, Pattern> descriptor = new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcastStream = patternStream.broadcast(descriptor);

        // 连接两条流进行处理
        actionStream.keyBy(data -> data.userId)
                .connect(broadcastStream)
                .process(new PatternDetector())
                .print();

        env.execute();
    }

    // 实现自定义的KeyedBroadcastProcessFunction
    public static class PatternDetector extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String,Pattern>> {
        // 保存上一次的action状态
        public ValueState<Action> actionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            actionState = getRuntimeContext().getState(new ValueStateDescriptor<Action>("action", Types.POJO(Action.class)));
        }

        @Override
        public void processElement(Action value, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            // 从广播状态中获取匹配模式
            ReadOnlyBroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(new MapStateDescriptor<Void, Pattern>("pattern", Types.VOID, Types.POJO(Pattern.class)));
            Pattern pattern = patternState.get(null);

            // 获取用户上一次的行为
            Action action = actionState.value();

            // 判断是否匹配
            if (action != null && pattern != null) {
                if (pattern.action1.equals(action.action) && pattern.action2.equals(value.action)) {
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }

            // 更新actionState
            actionState.update(value);
        }

        @Override
        public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            BroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(new MapStateDescriptor<Void, Pattern>("pattern", Types.VOID, Types.POJO(Pattern.class)));

            patternState.put(null, value);
        }
    }

    // 定义用户行为事件和模式的POJO类
    public static class Action {
        public String userId;
        public String action;

        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId='" + userId + '\'' +
                    ", login='" + action + '\'' +
                    '}';
        }
    }

    public static class Pattern {
        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }
}
