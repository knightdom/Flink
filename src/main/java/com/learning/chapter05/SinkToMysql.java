package com.learning.chapter05;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Flink - SinkToMysql
 *
 * @Author: Edgar Fang
 * @Date: 2022/11/7 23:32
 */
public class SinkToMysql {
    public static void main(String[] args) throws Exception {
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

        stream.addSink(JdbcSink.sink(
                "INSERT INTO clicks (user, url) VALUES (?, ?)",
                ((statement, event) -> {
                    statement.setString(1, event.user);
                    statement.setString(2, event.url);
                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://localhost:3306/test")
                .withDriverName("com.mysql.jdb.Driver")
                .withUsername("root")
                .withPassword("root")
                .build()
        ));

        env.execute();
    }
}
