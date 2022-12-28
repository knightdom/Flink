package com.learning.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created by Flink - TopNExample
 *
 * @Author: Edgar Fang
 * @Date: 2022/12/28 22:30
 */
public class TopNExample {
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

        // 普通Top N，选取当前所有用户中浏览量最大的2个
        Table topNResultTable = tableEnv.sqlQuery("SELECT `user`, cnt, row_num " +
                "FROM (" +
                "  SELECT *, ROW_NUMBER() OVER (" +
                "        ORDER BY cnt DESC" +
                "     ) AS row_num " +
                "  FROM ( SELECT `user`, COUNT(url) AS cnt FROM clickTable GROUP BY `user` )" +
                ") WHERE row_num <= 2");

        tableEnv.toChangelogStream(topNResultTable).print("top 2: ");

        // 窗口Top N，统计一段时间内的（前两名）活跃用户
        String subQuery = "SELECT `user`, COUNT(url) AS cnt, window_start, window_end " +
                "FROM TABLE (" +
                "   TUMBLE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ")" +
                " GROUP BY `user`, window_start, window_end";

        // 根据时间窗口统计top n，必须要在PARTITION BY后面添加window_start&window_end
        Table windowTopNResultTable = tableEnv.sqlQuery("SELECT `user`, cnt, row_num " +
                "FROM (" +
                "  SELECT *, ROW_NUMBER() OVER (" +
                "        PARTITION BY window_start, window_end " +
                "        ORDER BY cnt DESC" +
                "     ) AS row_num " +
                "  FROM ( " + subQuery + ")" +
                ") WHERE row_num <= 2");
        tableEnv.toDataStream(windowTopNResultTable).print(" window top n: ");

        env.execute();
    }
}
