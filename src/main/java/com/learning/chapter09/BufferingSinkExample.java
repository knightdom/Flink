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
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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
        env.enableCheckpointing(1000L); //每1s设置检查点，每1秒触发一次
        env.setStateBackend(new EmbeddedRocksDBStateBackend());  // 设置检查点保存到RockDB中，即保存到硬盘
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(""));    // 一般不直接在程序中配置，而是在配置文件中统一配置

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(60000L);  // 超过1分钟的话，抛弃该检查点
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);     // 模式有两种：至少一次，精确一次
        checkpointConfig.setMinPauseBetweenCheckpoints(500L);   // 第一个检查点的结束和第二个检查点的开始的时间间隔不得小于500ms
        checkpointConfig.setMaxConcurrentCheckpoints(1);    // 同时执行的检查点的个数，如果前一个执行时间太久，可能1s后还没完成，是否能生成第二个检查点
        checkpointConfig.enableUnalignedCheckpoints();  // 开启未对齐的检查点保存，即每个节点获得保存检查点的指令后就直接保存，不在需要等到整条业务线的节点都收到指令后才保存。前提条件：设置checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)，设置checkpointConfig.setMaxConcurrentCheckpoints(1)
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);  // 当手动取消了当前任务的时候，是否保存之前的检查点（当取消任务时删除检查点（默认），当取消任务时保留检查点
        checkpointConfig.setTolerableCheckpointFailureNumber(0);    // 该任务执行中允许检查点保存的失败次数，0：一直允许检查点保存失败

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
