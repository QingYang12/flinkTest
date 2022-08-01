package test.test02.broadcaststream;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.state.HeapBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test.test02.broadcaststream.util.DealRuleAnalysis;

import java.util.Properties;

/**BroadCastStream练习  使用广播实现配置动态更新
 * 接收 发送的配置，  接收 发送的数据
 * @ClassName test0201
 * @Description
 * @Author wanghao
 * @Date 2021/1/11 13:35
 * @Version 1.0
 */
public class TestBroadCastConsumer {
    public static void main(String[] args) {
        //final ParameterTool paramterTool=new ParameterTool.fromArgs(args);
        try {
            final Logger LOG = LoggerFactory.getLogger(TestBroadCastConsumer.class);
            final MapStateDescriptor<String, String> CONFIG_DESCRIPTOR = new MapStateDescriptor<>(
                    "wordsConfig",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

            Properties properties = new Properties();
            properties.put("group.id", "flink-kafka-connector");
            properties.put("bootstrap.servers", "127.0.0.1:9092");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // 执行环境并行度设置3
            env.setParallelism(3);
            DataStreamSource<String> configStream = env.addSource(new FlinkKafkaConsumer011<String>("config_bc_topic", new SimpleStringSchema(), properties));

            DataStreamSource<String> message = env.addSource(new FlinkKafkaConsumer011<String>("user_bc_topic", new SimpleStringSchema(), properties));
            //MapStateDescriptor<String, String> broadcastDescriptor = new MapStateDescriptor<>("boradcast-state", Types.STRING, Types.STRING);

            BroadcastStream<String> broadcastStream=configStream.broadcast(CONFIG_DESCRIPTOR);
            DataStream<String> connectedStream =message.connect(broadcastStream).process(new BroadcastProcessFunction<String, String, String>() {

                //数据处理流
                @Override
                public void processElement(String order, ReadOnlyContext ctx, Collector<String> collector) throws Exception {
                    HeapBroadcastState<String,String> config = (HeapBroadcastState)ctx.getBroadcastState(CONFIG_DESCRIPTOR);
                    String jsonobj=config.get("rule");
                    String res=DealRuleAnalysis.deal(order,jsonobj);
                    LOG.info("根据配置处理包装数据:" + res);
                    System.out.println("根据配置处理包装数据:" + res);
                    collector.collect(res);
                }
                //广播流
                @Override
                public void processBroadcastElement(String s, Context ctx, Collector<String> collector) throws Exception {
                    LOG.info("收到广播:" + s);
                    System.out.println("收到广播:" + s);
                    BroadcastState<String,String> state =  ctx.getBroadcastState(CONFIG_DESCRIPTOR);
                    ctx.getBroadcastState(CONFIG_DESCRIPTOR).put("rule",s);
                }
            });
            connectedStream.addSink((SinkFunction<String>) new FlinkKafkaProducer011<String>("stream_bc_topic", new SimpleStringSchema(), properties));

            env.execute("TestProcessConsumer kafka to kafka");
        } catch (Exception e) {
            e.printStackTrace();
        }


        

    }
}

