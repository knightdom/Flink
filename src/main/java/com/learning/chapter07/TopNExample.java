package com.learning.chapter07;

import com.learning.chapter05.ClickSource;
import com.learning.chapter05.Event;
import com.learning.chapter06.UrlCountViewExample;
import com.learning.chapter06.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * Created by Flink - TopNExample
 *
 * @Author: Edgar Fang
 * @Date: 2022/11/20 15:22
 */
public class TopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 1 按照url分组，统计窗口内每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(), new UrlCountViewExample.UrlViewCountResult());

        urlCountStream.print("url count");

        // 2 按照窗口的结束时间分组，统计并排序窗口内的url数量
        urlCountStream.keyBy(data -> data.windowEnd)
                .process(new TopNProcessResult(2))
                .print();

        env.execute();
    }

    public static class TopNProcessResult extends KeyedProcessFunction<Long, UrlViewCount, String> {
        // 定义属性
        private Integer n;
        // 定义列表状态
        private ListState<UrlViewCount> urlViewCountListState;

        public TopNProcessResult(int n) {
            this.n = n;
        }

        // 在环境中获取状态
        @Override
        public void open(Configuration parameters) throws Exception {
            getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-count-list", Types.POJO(UrlViewCount.class))
            );
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 将数据保存到状态中
            urlViewCountListState.add(value);
            // 注册windowEnd + 1ms的定时器 当前的key就是windowEnd的时间戳
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }

        // 在触发定时器时汇总

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCount urlViewCount: urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }

            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            // 包装信息打印输出
            StringBuilder result = new StringBuilder();
            result.append("-------------------\n");
            result.append("窗口结束事件：").append(new Timestamp(ctx.getCurrentKey())).append("\n");

            // 取list前两个，包装信息输出
            for (int i = 0; i < n; i++) {
                UrlViewCount currTuple = urlViewCountArrayList.get(i);
                String info = "No. " + (i + 1) + " "
                        + "url: " + currTuple.url + " "
                        + "访问量: " + currTuple.count + "\n";
                result.append(info);
            }
            result.append("-------------------\n");

            out.collect(result.toString());
        }
    }
}
