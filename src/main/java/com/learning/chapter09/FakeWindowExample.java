package com.learning.chapter09;

import com.learning.chapter05.ClickSource;
import com.learning.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * Created by Flink - FakeWindowExample
 *
 * @author Edgar Fang
 * @date 2022/12/1 16:20
 */
public class FakeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        stream.print("input");

        stream.keyBy(data -> data.url)
                .process(new FakeWindowResult(10000L))
                .print();

        env.execute();
    }

    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {
        MapState<Long, Long> urlCountWindowMapState;
        private final Long windowSize;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            urlCountWindowMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("url-count", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            Long windowStart = value.timestamp / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;

            // 配置定时器
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);

            if (urlCountWindowMapState.contains(windowStart)) {
                Long count = urlCountWindowMapState.get(windowStart);
                urlCountWindowMapState.put(windowStart, count + 1);
            } else {
                urlCountWindowMapState.put(windowStart, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;
            Long count = urlCountWindowMapState.get(windowStart);

            out.collect("窗口" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd) + " url: " + ctx.getCurrentKey() + " count: " + count);

            // 模拟窗口关闭，清除map中对应的key-value
            urlCountWindowMapState.remove(windowStart);
        }
    }
}
