package com.learning.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * Created by Flink - SinkToFileTest
 *
 * @Author: Edgar Fang
 * @Date: 2022/11/6 23:29
 */
public class SinkToFileTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

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

        // 自定义文件写入函数
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy( // 默认数据流滚动配置，就是什么时候这个文件可以关闭写下一个文件
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024)    // 允许文件的最大字节，超过就新建一个新的文件
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))    // 15分钟新建一个新的文件，withRoloverInterval()中参数默认单位为毫秒
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))   // 如果5分钟没有写入，就新建一个文件
                        .build()
                )
                .build();

        // 将数据写入文件
        stream.map(Event::toString)
                .addSink(streamingFileSink);

        env.execute();
    }
}
