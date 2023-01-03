package com.learning.chapter11;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

/**
 * Created by Flink - UdfTest_TableFunction
 *
 * @Author: Edgar Fang
 * @Date: 2023/1/3 15:22
 */
public class UdfTest_TableFunction {
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

        // 2 注册自定义表函数
        tableEnv.createTemporarySystemFunction("MySplit", MySplit.class);

        // 3 调用UDF进行查询转换
        Table resultTable = tableEnv.sqlQuery("select user, url, word, length " +
                "from clickTable, LATERAL TABLE( MySplit(url) ) AS T(word, length)");

        // 4 转换成流打印输出
        tableEnv.toDataStream(resultTable).print();

        env.execute();
    }

    // 实现自定义的表函数
    public static class MySplit extends TableFunction<Tuple2<String, Integer>>{
        public void eval(String str) {
            String[] fields = str.split("\\?");
            for (String field: fields) {
                collect(Tuple2.of(str, field.length()));
            }
        }
    }
}
