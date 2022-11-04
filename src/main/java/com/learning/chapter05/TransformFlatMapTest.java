package com.learning.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by Flink - TransformFlatMapTest
 *
 * @author Edgar Fang
 * @date 2022/11/4 16:15
 */
public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alic", "./prod", 3000L)
        );

        // 进行转换计算，提取user字段
        // 1 使用自定义类，实现FlatMapFunction接口
        stream.flatMap(new MyFlatMap()).print("1");

        // 2 使用匿名类实现MapFunction接口
        stream.flatMap(new FlatMapFunction<Event, String>() {

            @Override
            public void flatMap(Event value, Collector<String> out) throws Exception {
                out.collect(value.user);
                out.collect(value.url);
                out.collect(value.timestamp.toString());
            }
        }).print("2");

        // 3 lambda表达式实现
        stream.flatMap((Event value, Collector<String> out) -> {
            if (value.user.equals("Mary"))
                out.collect(value.url);
            else if (value.user.equals("Bob")) {
                out.collect(value.user);
                out.collect(value.url);
                out.collect(value.timestamp.toString());
            }
        }).returns(new TypeHint<String>() {})   // 范型擦除，所以需要声明
                .print("3");

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            out.collect(value.user);
            out.collect(value.url);
            out.collect(value.timestamp.toString());
        }
    }
}
