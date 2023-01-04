package com.learning.chapter11;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Created by Flink - UdfTest_TableAggregateFunction
 *
 * @Author: Edgar Fang
 * @Date: 2023/1/4 14:23
 */
public class UdfTest_TableAggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1 创建表
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

        // 2 注册自定义表聚合函数
        tableEnv.createTemporarySystemFunction("top2", top2.class);

        // 3 调用UDF进行查询转换
        String windowAggQuery = "SELECT user, COUNT(url) AS cnt, window_start, window_end " +
                "FROM TABLE (" +
                " TUMBLE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ")" +
                "GROUP BY user， window_start, window_end";

        Table aggTable = tableEnv.sqlQuery(windowAggQuery);

        // 由于sql对表聚合函数的兼容性不高，因此使用Table API
        // 将cnt字段的数据通过Top2表聚合函数进行统计，并将返回的二元组分别命名为value&rank
        Table resultTable = aggTable.groupBy($("window_end"))
                .flatAggregate(call("Top2", $("cnt")).as("value", "rank"))
                .select($("window_end"), $("value"), $("rank"));

        // 4 转换成流打印输出
        tableEnv.toChangelogStream(resultTable).print();


        env.execute();
    }

    public static class Top2Accumulator {
        public long max;
        public long secondMax;
    }

    public static class top2 extends TableAggregateFunction<Tuple2<Long, Integer>, Top2Accumulator> {
        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator top2Accumulator = new Top2Accumulator();
            top2Accumulator.max = Long.MIN_VALUE;
            top2Accumulator.secondMax = Long.MIN_VALUE;
            return top2Accumulator;
        }

        // 定义一个更新累加器的方法
        public void accumulate(Top2Accumulator accumulator, Long value) {
            if (value > accumulator.max) {
                accumulator.secondMax = accumulator.max;
                accumulator.max = value;
            } else if (value > accumulator.secondMax) {
                accumulator.secondMax = value;
            }
        }

        // 输出结果，当前的top 2
        public void emitValue(Top2Accumulator accumulator, Collector<Tuple2<Long, Integer>> out) {
            if (accumulator.max != Long.MIN_VALUE) {
                out.collect(Tuple2.of(accumulator.max, 1));
            }
            if (accumulator.secondMax != Long.MIN_VALUE) {
                out.collect(Tuple2.of(accumulator.secondMax, 2));
            }
        }
    }
}
