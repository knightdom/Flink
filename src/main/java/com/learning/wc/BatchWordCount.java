package com.learning.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Created by Flink - BatchWordCount
 *
 * @author Edgar Fang
 * @date 2022/8/25 16:02
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2 从文件中读取数据
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");

        // 3 将每行数据进行分词，转换成二元组类型
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 将一行文本进行分词
            String[] words = line.split(" ");
            // 将每个单词转换成二元组输出
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
                // 类型声明
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4 按照word进行分组（根据二元组中的索引为0的数据进行分组）
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);

        // 5 分组内进行聚合统计（根据二元组中的索引为1的数据进行聚合统计）
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        // 6 dayin结果
        sum.print();
    }
}
