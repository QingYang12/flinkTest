package test.test01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**读取文件做   File统计个数
 * @ClassName test01
 * @Description TODO   File count word
 * @Author wanghao628
 * @Date 2021/1/8 16:14
 * @Version 1.0
 */
public class Test01 {
    public static void main(String[] args) {

        try{
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            DataSource<String> ds = env.readTextFile("D:\\tttt\\new2.txt");
            ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                           public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                               String[] strarr = s.split(",");
                               for (String item : strarr) {
                                   collector.collect(new Tuple2<String, Integer>(item,1));
                               }
                           }
                       }
            ).groupBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                    return new Tuple2<String, Integer>(stringIntegerTuple2.f0,stringIntegerTuple2.f1+ t1.f1) ;
                }
            }).print();
        }catch (Exception e){
                e.printStackTrace();
        }


    }



}
