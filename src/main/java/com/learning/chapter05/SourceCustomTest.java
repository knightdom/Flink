package com.learning.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Flink - SourceCustomTest
 *
 * @Author: Edgar Fang
 * @Date: 2022/10/30 23:14
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> customStream = env.addSource(new ClickSource());

        customStream.print();

        env.execute();
    }
}
