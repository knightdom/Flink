package com.learning.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by Flink - StreamWourdCount
 *
 * @author Edgar Fang
 * @date 2022/8/29 11:47
 */
public class StreamWourdCount {
    public static void main(String[] args) throws Exception {
        // 1 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从参数重提取主机名和端口号
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");

        // 2 读取文本流
        DataStreamSource<String> lineDataStream = env.socketTextStream(host, port);// 从指定主机上读取文本流

        // 3 转换计算（和批处理一样）
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);   // keyBy()建议添加KeySelector，因此用一个lambda表达式获取

        // 5 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

        // 6 求和
        sum.print();

        // 7 启动执行 流处理中1-6步骤只是制定了数据处理流程，但是流是无界的，因此需要启动这个流程，相当于一个监听进程
        env.execute();
    }
}
