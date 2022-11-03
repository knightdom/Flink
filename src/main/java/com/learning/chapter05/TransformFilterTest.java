package com.learning.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Flink - TransformFilterTest
 *
 * @Author: Edgar Fang
 * @Date: 2022/11/3 23:34
 */
public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 进行过滤计算，比较user字段
        // 1 使用自定义类，实现FilterFunction接口
        SingleOutputStreamOperator<Event> result = stream.filter(new Myfilter());

        // 2 使用匿名类实现MapFunction接口
        SingleOutputStreamOperator<Event> result2 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Bob");
            }
        });

        // 3 使用lambda表达式实现
        SingleOutputStreamOperator<Event> result3 = stream.filter(event -> event.user.equals("Mary"));

        result3.print();

        env.execute();
    }

    public static class Myfilter implements FilterFunction<Event> {

        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("Mary");
        }
    }
}
