package test.test01;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**读取文件做统计单词个数
 * @ClassName test0101
 * @Description TODO File count word3
 * @Author wanghao   空格分隔算单词  统计所有单词出现的数量  批量
 * 1.符号替换成空格
 * 2.去除key为null 或者""空字符串的数据
 * 3.统计单词个数
 * @Date 2021/1/8 16:14
 * @Version 1.0
 */
public class Test0102 {
    public static void main(String[] args) {

        try{
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            DataSource<String> ds = env.readTextFile("D:\\tttt\\Test0102.txt");
            ds.map(new MapFunction<String, String>() {
                //处理掉所有的符号
                @Override
                public String map(String s) throws Exception {
                    s = s.replaceAll("[\\pP\\p{Punct}]", " ");
                    return s;
                }
            }).flatMap(new FlatMapFunction<String,Tuple2<String,Integer>>() {
                //包装成jsonObject对象
                @Override
                public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
                     String[] strWorldArr =s.split(" ");
                     for(String item:strWorldArr){
                         boolean key=hasChineseCharacter(item);
                         if(key){//包含汉字
                             for(int i=0;i<=item.length()-1;i++){
                                 String str=""+ item.charAt(i);
                                 collector.collect(new Tuple2<String,Integer>(str,1));
                             }

                         }else{//不包含汉字
                             collector.collect(new Tuple2<String,Integer>(item,1));
                         }

                     }
                }
            }).groupBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t0, Tuple2<String, Integer> t1) throws Exception {
                    return new Tuple2<String, Integer>(t0.f0,t0.f1+ t1.f1) ;
                }
            }).map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> s) throws Exception {
                        if(hasChineseCharacter(s.f0)){
                            s.f0="chinaChar";
                        }else{
                            s.f0="other";
                        }
                        return s;
                    }
                }
            ).groupBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t0, Tuple2<String, Integer> t1) throws Exception {
                    Tuple2<String, Integer> result=null;
                    if(hasChineseCharacter(t1.f0)){
                        result=new  Tuple2<String, Integer>(t1.f0,t0.f1+ t1.f1) ;
                    }else{
                        result=new Tuple2<String, Integer>(t1.f0,t0.f1+ t1.f1) ;
                    }
                    return new Tuple2<String, Integer>(t1.f0,t0.f1+ t1.f1) ;
                }
            }).print();
        }catch (Exception e){
                e.printStackTrace();
        }


    }
    /**
     * 判断一个字符串是否包含中文
     * */
    public static boolean hasChineseCharacter(String str) {
        if (str == null) return false;
        for (char c : str.toCharArray()) {
            if (isChineseCharacter(c)) return true;// 有一个中文字符就返回
        }
        return false;
    }
    /**
     * 判断一个字符是否是中文
     * */
    public static boolean isChineseCharacter(char c) {
        return c >= 0x4E00 && c <= 0x9FA5;// 根据字节码判断
    }



}
