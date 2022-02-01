package test.test02.transformation.process;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**process练习  加 旁路输出  流拆分
 * @ClassName test0201
 * @Description TODO kafka 到 db
 * @Author wanghao
 * @Date 2021/1/11 13:35
 * @Version 1.0
 */
public class TestProcessConsumer {
    public static void main(String[] args) {
        //final ParameterTool paramterTool=new ParameterTool.fromArgs(args);
        try {
            final Logger LOG = LoggerFactory.getLogger(TestProcessConsumer.class);

            Properties properties = new Properties();
            properties.put("group.id", "flink-kafka-connector");
            properties.put("bootstrap.servers", "127.0.0.1:9092");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // 执行环境并行度设置3
            env.setParallelism(3);
            DataStreamSource<String> message = env.addSource(new FlinkKafkaConsumer011<String>("process_test1", new SimpleStringSchema(), properties));

            //旁路输出，拆分流
            OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

            SingleOutputStreamOperator messagestreaming=message.process(new ProcessFunction<String, String>() {
                @Override
                public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                    LOG.info("已收到 并转换完成：" + s);
                    System.out.println("已收到 并转换完成：" + s);
                    context.output(outputTag,s);
                    collector.collect(s);
                }
            });
            DataStream<String> sideOutputStream = messagestreaming.getSideOutput(outputTag);//获取sideOutput的数据
            sideOutputStream.map(new MapFunction<String, String>() {
                @Override
                public String map(String s) throws Exception {
                    LOG.info("旁路--已收到 并转换完成：" + s);
                    System.out.println("旁路--已收到 并转换完成：" + s);
                    return s;
                }
            });
            sideOutputStream.addSink((SinkFunction<String>) new FlinkKafkaProducer011<String>("process_test3", new SimpleStringSchema(), properties));


            messagestreaming.addSink((SinkFunction<String>) new FlinkKafkaProducer011<String>("process_test2", new SimpleStringSchema(), properties));


            env.execute("TestProcessConsumer kafka to kafka");
        } catch (Exception e) {
            e.printStackTrace();
        }


        

    }
}

