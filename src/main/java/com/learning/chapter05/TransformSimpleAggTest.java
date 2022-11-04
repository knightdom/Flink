package com.learning.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Flink - TransformSimpleAggTest
 *
 * @author Edgar Fang
 * @date 2022/11/4 16:58
 */
public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 获取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alic", "./prod", 3000L),
                new Event("Bob", "./prod?id=2", 3500L),
                new Event("Alice", "./cart", 3600L),
                new Event("Bob", "./home", 3800L),
                new Event("Bob", "./cart", 4000L)
        );

        // max仅修改指定字段的值变成最大值
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp")
                .print("max: ");

        // maxBy修改的是整个数据项的内容都变成最大值的那条
        stream.keyBy(data -> data.user)
                .maxBy("timestamp")
                .print("maxBy: ");

        env.execute();
    }
}
