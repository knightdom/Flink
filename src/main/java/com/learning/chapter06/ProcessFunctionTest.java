package com.learning.chapter06;

import com.learning.chapter05.ClickSource;
import com.learning.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Created by Flink - ProcessFunctionTest
 *
 * @author Edgar Fang
 * @date 2022/11/18 14:51
 */
public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                if (value.user.equals("Mary")) {
                    out.collect(value.user + "clicks" + value.url);
                } else if (value.user.equals("Bob")) {
                    out.collect(value.user);
                    out.collect(value.user);
                }
                out.collect(value.toString());
                System.out.println("timestamp: " + ctx.timestamp());
                System.out.println("watermark: " + ctx.timerService().currentWatermark());

                System.out.println(getRuntimeContext().getIndexOfThisSubtask());    // 获取当前子任务的索引
            }
        }).print();

        env.execute();
    }
}
