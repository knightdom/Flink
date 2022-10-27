package com.learning.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by Flink - BoundedStreamWordCount
 *
 * @author Edgar Fang
 * @date 2022/8/29 11:02
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2 读取文件
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");

        // 3 转换计算（和批处理一样）
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4 分组 在二元组中，f0表示第一个元素，f1表示第二个元素。以data中的第一个元素为key
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);   // keyBy()建议添加KeySelector，因此用一个lambda表达式获取

        // 5 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

        // 6 求和
        sum.print();

        // 7 启动执行 流处理中1-6步骤只是制定了数据处理流程，但是流应该是无界的，因此需要启动这个流程并在数据进来的时候执行这个流程
        env.execute();
    }
}
