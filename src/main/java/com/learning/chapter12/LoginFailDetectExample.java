package com.learning.chapter12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Created by Flink - LoginFailDetectExample
 *
 * @Author: Edgar Fang
 * @Date: 2023/1/4 21:15
 */
public class LoginFailDetectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<LoginEvent> lgoinEventStream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 2 定义模式，连续三次登录失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first")  // 第一次登录失败
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("second")  // 第二次登录失败
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("third")  // 第三次登录失败
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                });

        // 3 将模式应用到数据流上，检测复杂事件
        PatternStream<LoginEvent> patternStream = CEP.pattern(lgoinEventStream.keyBy(data -> data.userId), pattern);

        // 4 将检测到的复杂事件提取出来，进行处理得到报警信息输出
        SingleOutputStreamOperator<String> warningStream = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            // Map中的key（String类型）就是在定义模式中的"first", "second" ...
            // value是List类型，主要是考虑到有些事件需要定义重复发生，目前由于不需要，因此是只有一个元素的list
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                // 提取复杂事件中的三次登录失败事件
                LoginEvent firstFailEvent = map.get("first").get(0);    // get(0)是获取list中第1个元素
                LoginEvent secondFailEvent = map.get("second").get(0);
                LoginEvent thirdFailEvent = map.get("third").get(0);

                return firstFailEvent.userId + " 连续三次登录失败！登录时间：" +
                        firstFailEvent.timestamp + ", " +
                        secondFailEvent.timestamp + ", " +
                        thirdFailEvent.timestamp;
            }
        });

        // 打印输出
        warningStream.print();

        env.execute();
    }
}