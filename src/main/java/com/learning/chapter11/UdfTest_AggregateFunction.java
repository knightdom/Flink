package com.learning.chapter11;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Created by Flink - UdfTest_AggregateFunction
 *
 * @Author: Edgar Fang
 * @Date: 2023/1/3 20:14
 */
public class UdfTest_AggregateFunction {
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

        // 2 注册自定义聚合函数
        tableEnv.createTemporarySystemFunction("WeightedAverage", WeightedAverage.class);

        // 3 调用UDF进行查询转换
        Table resultTable = tableEnv.sqlQuery("select user, WeightedAverage(ts, 1) as w_avg " +
                "from clickTable group by user");

        // 4 转换成流打印输出
        tableEnv.toChangelogStream(resultTable).print();

        env.execute();
    }

    // POJO类，实现累加器中的总和和次数
    public static class WeightedAvgAccumulator {
        public long sum = 0;
        public int count = 0;
    }

    public static class WeightedAverage extends AggregateFunction<Long, WeightedAvgAccumulator> {
        @Override
        public Long getValue(WeightedAvgAccumulator weightedAvgAccumulator) {
            if (weightedAvgAccumulator.count == 0) {
                return null;
            } else {
                return weightedAvgAccumulator.sum / weightedAvgAccumulator.count;
            }
        }

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        // 实现累加计算的方法
        // 必须实现accumulate()，参数也有规定，参数1：中间聚合结果类型；后面的参数：调用累加器时，需要额外传入的参数
        public void accumulate(WeightedAvgAccumulator accumulator, Long iValue, Integer iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }
    }
}
