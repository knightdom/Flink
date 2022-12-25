package com.learning.chapter11;

import com.learning.chapter05.ClickSource;
import com.learning.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Created by Flink - SimpleTableExample
 *
 * @Author: Edgar Fang
 * @Date: 2022/12/12 22:17
 */
public class SimpleTableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1 获取数据
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 2 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3 将DataStream转换成Table
        Table eventTable = tableEnv.fromDataStream(eventStream);

        // 4 直接写SQL进行转换
        Table resultTable1 = tableEnv.sqlQuery("select user, url, `timestamp` from " + eventTable);

        // 5 基于Table直接转换
        Table resultTable2 = eventTable.select($("user"), $("url"))
                .where($("user").isEqual("Alice"));

        // 6 转换成流打印输出
        tableEnv.toDataStream(resultTable1).print("result1");
        tableEnv.toDataStream(resultTable2).print("result2");

        // 7 聚合转换
        tableEnv.createTemporaryView("clickTable", eventTable);
        Table aggResult = tableEnv.sqlQuery("select user, COUNT(url) as cnt from clickTable group by user");

//        tableEnv.toDataStream(aggResult).print("agg");    // 会报错，因为toDataStream()仅会插入数据，无法更新数据，一般聚合都是需要更新数据
        tableEnv.toChangelogStream(aggResult).print("agg"); // 更新时，先删除原有的数据再插入新的数据

        env.execute();
    }
}
