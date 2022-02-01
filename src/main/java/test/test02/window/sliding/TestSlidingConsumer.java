package test.test02.window.sliding;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

/**
 * @author
 * @title: TestConsumer
 * @projectName flinkTest
 * @description:   统计用户 5分钟内访问情况
 * @date 2022/1/52:49 下午
 */
public class TestSlidingConsumer {

    public static void main(String[] args) {

        try {
            final Logger LOG = LoggerFactory.getLogger(TestSlidingConsumer.class);

            Properties properties = new Properties();
            properties.put("group.id", "flink-kafka-connector");
            properties.put("bootstrap.servers", "127.0.0.1:9092");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStreamSource<String> message = env.addSource(new FlinkKafkaConsumer011<String>("test_sliding_window_1", new SimpleStringSchema(), properties));
            message.map(new MapFunction<String, JSONObject>() {
                            @Override
                            public JSONObject map(String s) throws Exception {

                                JSONObject jsonObject=JSONObject.parseObject(s);
                                String user=jsonObject.getString("user");
                                String count=jsonObject.getString("count");
                                JSONObject root=new JSONObject();//usercount
                                root.put("user",user);
                                root.put("count",count);
                                SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
                                Date date=new Date();
                                root.put("time",sdf.format(date));
                                LOG.info("已收到 并转换完成：" + root.toString());
                                System.out.println("已收到 并转换完成：" + root.toString());
                                return root;
                            }
                        }

            ).keyBy(new KeySelector<JSONObject, String>() {
                 @Override
                 public String getKey(JSONObject line) throws Exception {
                     return line.getString("user");
                 }
             })
            .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(1)))
            .reduce(new ReduceFunction<JSONObject>() {
                        @Override
                        public JSONObject reduce(JSONObject t0, JSONObject t1) throws Exception {
                            String count =String.valueOf(Integer.valueOf(t0.getString("count")) +Integer.valueOf(t1.getString("count")));
                            System.out.println("已处理：" + t0.get("user") + "---" + count);
                            LOG.info("已处理：" + t0.get("user") + "---" + count);
                            JSONObject root=new JSONObject();//usercount
                            root.put("user",t0.get("user"));
                            root.put("count",count);
                            root.put("time",t1.getString("time"));
                            return root ;
                        }
                    }
            ).addSink(new ConnectMySqlSlidingSink());
            env.execute("Test02consumer kafka to db");
        } catch (Exception e) {
            e.printStackTrace();
        }



    }


}
