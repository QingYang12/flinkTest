package test.test02.a_simple;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**从消息队列读取统计单词个数   topic
 * @ClassName test02
 * @Description TODO  kafka count word
 * topic:   test1
 * @Author wanghao
 * @Date 2021/1/11 13:35
 * @Version 1.0
 */
public class Test02consumer {
    public static void main(String[] args) {
        //final ParameterTool paramterTool=new ParameterTool.fromArgs(args);
        try {
            final Logger LOG = LoggerFactory.getLogger(Test02consumer.class);

            Properties properties = new Properties();
            properties.put("group.id", "flink-kafka-connector");
            properties.put("bootstrap.servers", "127.0.0.1:9092");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStreamSource<String> message = env.addSource(new FlinkKafkaConsumer011<String>("test1", new SimpleStringSchema(), properties));
            message.map(new MapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(String s) throws Exception {
                                Tuple2 tuple2 = new Tuple2<String, Integer>(s, 1);
                                System.out.println("已接收：" + s);
                                LOG.info("已接收：" + s);
                                return tuple2;
                            }
                        }

            )
                    .keyBy(0)
                    .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                                @Override
                                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t0, Tuple2<String, Integer> t1) throws Exception {
                                    System.out.println("已处理：" + t0.f0 + "---" + t0.f1 + t1.f1);
                                    LOG.info("已处理：" + t0.f0 + "---" + t0.f1 + t1.f1);
                                    return new Tuple2<String, Integer>(t0.f0, t0.f1 + t1.f1);
                                }
                            }
                    )
                    .print();
            env.execute("Test02consumer cd");
        } catch (Exception e) {
            e.printStackTrace();
        }


        

    }
}

