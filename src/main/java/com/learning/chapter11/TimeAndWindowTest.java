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
 * Created by Flink - TimeAndWindowTest
 *
 * @Author: Edgar Fang
 * @Date: 2022/12/25 15:45
 */
public class TimeAndWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1 在创建表的DDL中直接定义时间属性
        // 注意TO_TIMESTAMP函数是将字符串类型时间(STRING类型)转换成时间戳(TIMESTAMP类型)，FROM_UNIXTIME函数是将整型时间(INT/BIGINT类型转换成字符串类型时间）
        // 在flink中整型的时间无法直接在watermark中使用，需要通过TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) )转换
        String createDDL = "CREATE TABLE clickTable (" +
                " `user` STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) ), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")";
        tableEnv.executeSql(createDDL);
        
        // 2 在流转换成Table的时候定义时间属性
        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        Table clickTable = tableEnv.fromDataStream(clickStream, $("user"), $("url"), $("timestamp").as("ts"), $("et").rowtime());

//        clickTable.printSchema();

        // 3 聚合查询转换
        // 3.1 分组聚合 (该sql语句中的clickTable是craeteDDL中的clickTable，下面的那个clickTable是一个table对象，无法直接用于sql语句中)
        Table aggTable = tableEnv.sqlQuery("select `user`, count(1) from clickTable group by `user`");

        // 3.2 窗口聚合
        // 3.2.1 滑动窗口
        Table tumbleWindowResultTable = tableEnv.sqlQuery("select `user`, count(1) as cnt, " +
                " window_end as endT " +
                "from TABLE( " +
                " TUMBLE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ") " +
                "GROUP BY `user`, window_end, window_start"
        );
        // 3.2.2 滚动窗口
        Table hopWindowResultTable = tableEnv.sqlQuery("select `user`, count(1) as cnt, " +
                " window_end as endT " +
                "from TABLE( " +
                " HOP(TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ") " +
                "GROUP BY `user`, window_end, window_start"
        );
        // 3.2.3 累积窗口
        Table cumulateWindowResultTable = tableEnv.sqlQuery("select `user`, count(1) as cnt, " +
                " window_end as endT " +
                "from TABLE( " +
                " CUMULATE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ") " +
                "GROUP BY `user`, window_end, window_start"
        );
        // 3.3 开窗聚合 (Over)
        Table overWindowResultTable = tableEnv.sqlQuery("select `user`, " +
                " avg(ts) OVER(" +
                "  PARTITION BY `user` " +
                "  ORDER BY et " +
                "  ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" +
                ") AS avg_ts " +
                "FROM clickTable"
        );
        // 如果有多个聚合统计根据同一个over窗口
        Table overWindowResultTable1 = tableEnv.sqlQuery("select `user`, " +
                " avg(ts) OVER w AS avg, " +
                " MAX(CHAR_LENGTH(url)) OVER w AS max_url" +
                "FROM clickTable" +
                "WINDOW w AS (" +
                " PARTITION BY `user` " +
                " ORDER BY ts " +
                " ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" +
                ")");


        tableEnv.toChangelogStream(aggTable).print("agg");
        tableEnv.toDataStream(tumbleWindowResultTable).print("tumble window: ");
        tableEnv.toDataStream(hopWindowResultTable).print("hop window: ");
        tableEnv.toDataStream(cumulateWindowResultTable).print("cumulate window: ");
        tableEnv.toDataStream(overWindowResultTable).print("over window: ");
        tableEnv.toDataStream(overWindowResultTable1).print("over window1: ");

        env.execute();
    }
}
