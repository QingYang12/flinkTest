package test.test02;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**落库 从topic到topic  test_word1 到 test_word2
 * @ClassName test0202
 * @Description TODO kafka 到 kafka
 * @Author wanghao628
 * @Date 2021/1/11 13:35
 * @Version 1.0
 */
public class Test0202consumer {
    public static void main(String[] args) {
        //final ParameterTool paramterTool=new ParameterTool.fromArgs(args);
        try {
            final Logger LOG = LoggerFactory.getLogger(Test0202consumer.class);

            Properties properties = new Properties();
            properties.put("group.id", "flink-kafka-connector");
            properties.put("bootstrap.servers", "127.0.0.1:9092");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStreamSource<String> message = env.addSource(new FlinkKafkaConsumer011<String>("test_word1", new SimpleStringSchema(), properties));
            message.map(new MapFunction<String,String>() {
                            @Override
                            public String map(String s) throws Exception {
                                System.out.println("已接收：" + s);
                                LOG.info("已接收：" + s);
                                return s;
                            }
                        }

            ).addSink((SinkFunction<String>) new FlinkKafkaProducer011<String>("test_word2", new SimpleStringSchema(), properties));
            env.execute("Test02consumer cd");
        } catch (Exception e) {
            e.printStackTrace();
        }


        

    }
}

