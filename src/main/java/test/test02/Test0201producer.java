package test.test02;


import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test.test02.util.ConnectMySqlSource;
import test.test02.util.SourceVo;

import java.util.Properties;

/**从数据库读取字段发送到topic  数据库 dbtest01 到 topic:db_kafka_topic
 * @ClassName test0201
 * @Description TODO db 到 kafka
 * @Author wanghao
 * @Date 2021/1/11 13:35
 * @Version 1.0
 */
public class Test0201producer {
    public static void main(String[] args) {
        //final ParameterTool paramterTool=new ParameterTool.fromArgs(args);
        try {
            final Logger LOG = LoggerFactory.getLogger(Test0201producer.class);

            Properties properties = new Properties();
            properties.put("group.id", "flink-kafka-connector");
            properties.put("bootstrap.servers", "127.0.0.1:9092");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStreamSource<SourceVo> source =  env.addSource(new ConnectMySqlSource());
            source.map(new MapFunction<SourceVo, String>() {
                @Override
                public String map(SourceVo sourceVo) throws Exception {
                    String sourceVoStr=JSON.toJSON(sourceVo).toString();
                    System.out.println("已读取 并发送到db_kafka_topic：" + sourceVoStr);
                    LOG.info("已读取 并发送到db_kafka_topic：" + sourceVoStr);
                    return sourceVoStr;
                }
            }).addSink((SinkFunction<String>) new FlinkKafkaProducer011<String>("db_kafka_topic", new SimpleStringSchema(), properties));
            env.execute("Test02consumer db to kafka");
        } catch (Exception e) {
            e.printStackTrace();
        }


        

    }
}

