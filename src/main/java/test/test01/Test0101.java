package test.test01;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Set;

/**读取文件做统计品类个数   file品类个数统计
 * @ClassName test0101
 * @Description TODO  File count word2
 * @Author wanghao628  统计品类个数  批量
 * @Date 2021/1/8 16:14
 * @Version 1.0
 */
public class Test0101 {
    public static void main(String[] args) {

        try{
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            DataSource<String> ds = env.readTextFile("D:\\tttt\\test0101.txt");
            ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                           public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                               String[] ObjectStrArr = s.split(";");
                               for(String  objectStr:ObjectStrArr){
                                   JSONObject jsonObject= JSON.parseObject(objectStr);
                                   Set<String> keys=jsonObject.keySet();
                                   Iterator<String> iterator = keys.iterator();
                                   while(iterator.hasNext()){
                                       String next = iterator.next();
                                       String category=jsonObject.getString(next);
                                       collector.collect(new Tuple2<String, Integer>(category,1));
                                   }

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
