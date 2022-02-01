package test.test02;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test.test02.util.ConnectMySqlSink;
import test.test02.util.ConnectMySqlSource;

import java.util.Properties;

/**从消息队列读取字段落进数据库  topic: db_kafka_topic 到 数据库 dbtest02
 * @ClassName test0201
 * @Description TODO kafka 到 db
 * @Author wanghao
 * @Date 2021/1/11 13:35
 * @Version 1.0
 */
public class Test0201consumer {
    public static void main(String[] args) {
        //final ParameterTool paramterTool=new ParameterTool.fromArgs(args);
        try {
            final Logger LOG = LoggerFactory.getLogger(Test0201consumer.class);

            Properties properties = new Properties();
            properties.put("group.id", "flink-kafka-connector");
            properties.put("bootstrap.servers", "127.0.0.1:9092");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStreamSource<String> message = env.addSource(new FlinkKafkaConsumer011<String>("db_kafka_topic", new SimpleStringSchema(), properties));
            message.map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String s) throws Exception {
                        System.out.println("已收到 并入库：" + s);
                        LOG.info("已收到 并入库：" + s);
                        JSONObject jsonObject=JSONObject.parseObject(s);
                        String id=jsonObject.getString("id");
                        String name=jsonObject.getString("name");
                        String old=jsonObject.getString("old");
                        String valuex=jsonObject.getString("valuex");
                        JSONObject root=new JSONObject();
                        root.put("id",id);
                        root.put("name",name);
                        root.put("value_name",old+valuex);

                        return root;
                    }
                }

            ).addSink(new ConnectMySqlSink());
            env.execute("Test02consumer kafka to db");
        } catch (Exception e) {
            e.printStackTrace();
        }


        

    }
}

