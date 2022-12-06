package com.learning.chapter09;

import com.learning.chapter05.ClickSource;
import com.learning.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Flink - BufferingSinkExample
 *
 * @author Edgar Fang
 * @date 2022/12/5 17:32
 */
public class BufferingSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置检查点
        env.enableCheckpointing(1000L); //每10s设置检查点
        env.setStateBackend(new EmbeddedRocksDBStateBackend());  // 设置检查点保存到RockDB中，即保存到硬盘


        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        stream.print("input");

        // 批量缓存输出
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        // 定义当前类的属性
        private final int threshold;
        private List<Event> bufferedElements;
        // 定义一个算子状态
        private ListState<Event> checkpointedState;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        // 在使用快照进行状态的持久化时，调用该方法。因此需要将bufferedElements和checkpointedState关联起来
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();

            // 对状态进行持久化，将bufferedElements和checkpointedState关联起来
            for (Event element: bufferedElements) {
                checkpointedState.add(element);
            }
        }

        // 初始化算子状态 或者 从快照中恢复算子状态
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 定义算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-elements", Event.class);

            // 将初始化的状态赋给checkpointedState，如果时故障恢复，就是从快照中将状态赋给checkpointedState
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

             // 如果从故障恢复，需要将ListState中的所有元素复制到列表中
            if (context.isRestored()) { // 是否是从故障中恢复的
                for (Event element: checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }

        // 批量写入
        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);    // 缓存到列表

            // 判断如果达到阈值，就批量写入
            if (bufferedElements.size() == threshold) {
                for (Event element: bufferedElements) {
                    System.out.println(element);
                }

                System.out.println("==========输出完毕=========");
                bufferedElements.clear();
            }
        }
    }
}
