package com.learning.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformPartitionTest {
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
                new Event("Bob", "./cart", 4000L),
                new Event("Mary", "./cart", 3600L)
        );

        // 1. 随机分区
//        stream.shuffle().print().setParallelism(4);

        // 2. 轮询分区（并行度不同时的默认分区机制）
//        stream.rebalance().print().setParallelism(4);
//        stream.print().setParallelism(4);

        // 3. rescale
        // 和轮询分区类似，但是是在各自的分区内进行轮询分区
        // 例如，前一个子任务a并行度是2，后一个子任务b并行度是4
        // 如果是rebalance的话，a1会将数据按顺序分配给b1，b2，b3，b4
        // 如果是rescale，a1会将数据分配给b1和b2，a2会将数据分配给b3，b4
        // 3.1 获取数据
        DataStreamSource<Integer> stream1 = env.addSource(new RichParallelSourceFunction<Integer>() {

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 0; i < 8; i++) {
                    // 将奇偶数分别发送到0号和1号的并行分区中
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask())
                        sourceContext.collect(i);
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2);

//        stream1.rescale().print().setParallelism(4);
        /*
        * 结果可以看到，所有的偶数都是在1和2两个分区中，而所有的奇数都在3和4中
        * 4> 3
        * 3> 1
        * 1> 0
        * 2> 2
        * 1> 4
        * 3> 5
        * 4> 7
        * 2> 6
        */

        // 4. 广播 将数据发送给所有分区
//        stream.broadcast().print().setParallelism(4);

        // 5. 全局 将所有的数据发送到一个分区中
//        stream.global().print().setParallelism(4);

        // 6. 自定义重分区
        

        env.execute();
    }
}
