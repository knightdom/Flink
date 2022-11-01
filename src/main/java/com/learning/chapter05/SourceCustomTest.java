package com.learning.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

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

        // 获取自定义数据源
//        DataStreamSource<Event> customStream = env.addSource(new ClickSource());

        // 获取并行的自定义数据源
        DataStreamSource<Integer> customStream = env.addSource(new ParallelCustomSource()).setParallelism(4);

        customStream.print();

        env.execute();
    }

    public static class ParallelCustomSource implements ParallelSourceFunction<Integer> {
        private Boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(random.nextInt());
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
