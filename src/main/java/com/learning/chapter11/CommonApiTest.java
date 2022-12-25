package com.learning.chapter11;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Created by Flink - CommonApiTest
 *
 * @Author: Edgar Fang
 * @Date: 2022/12/15 22:21
 */
public class CommonApiTest {
    public static void main(String[] args) throws Exception {
        // 1.1 依赖StreamExecutionEnvironment创建表执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 1.2 定义环境配置来创建表执行环境
        // 1.2.1 基于blink版本planner进行流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 1.2.2 基于老版本planner进行流处理
//        EnvironmentSettings settings1 = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .useOldPlanner()
//                .build();
//        TableEnvironment tableEnv1 = TableEnvironment.create(settings1);
//
//        // 1.2.3 基于老版本planner进行批处理（由于老版本时，批流未统一）
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv);
//
//        // 1.2.4 基于blink版本planner进行批处理
//        EnvironmentSettings settings3 = EnvironmentSettings.newInstance()
//                .inBatchMode()
//                .useBlinkPlanner()
//                .build();
//        TableEnvironment tableEnv3 = TableEnvironment.create(settings3);

        // 2 创建连接器表
        String createDDL = "CREATE TABLE clickTable (" +
                " `user` STRING, " +
                " url STRING, " +
                " ts BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")";

        tableEnv.executeSql(createDDL);

        // 3 查询
        // 3.1 调用Table API进行表的查询转换
        Table clickTable = tableEnv.from("clickTable");
        Table resultTable = clickTable.where($("user").isEqual("Bob"))
                .select($("user"), $("url"));

        tableEnv.createTemporaryView("result2", resultTable);
        // 3.2 执行sql进行表的查询转换
        Table resultTable2 = tableEnv.sqlQuery("select url, `user` from result2");

        // 5 行聚合计算的查询转换
        Table aggResult = tableEnv.sqlQuery("select `user`, COUNT(1) as cnt from clickTable group by `user`");

        // 4 输出表
        // 4.1 创建一张用于输出的表 输出的path只需要填写目录，因为如果有多个并行度，会生成多张表
        String createOutDDL = "CREATE TABLE outTable (" +
                " `user` STRING, " +
                " url STRING " +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'output'," +
                " 'format' = 'csv'" +
                ")";
        tableEnv.executeSql(createOutDDL);

        // 4.2 创建一张用于控制台打印输出的表
        String createPrintOutDDL = "CREATE TABLE printOutTable (" +
                " `user` STRING, " +
                " cnt BIGINT " +
                ") WITH (" +
                " 'onnector' = 'print'" +
                ")";
        tableEnv.executeSql(createPrintOutDDL);

        // 输出
//        resultTable.executeInsert("outTable");
//        resultTable2.executeInsert("printOutTable");
        aggResult.executeInsert("printOutTable");
    }
}