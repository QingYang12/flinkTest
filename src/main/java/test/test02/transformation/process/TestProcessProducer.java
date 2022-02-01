package test.test02.transformation.process;


import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test.test02.Test02producer;
import test.test02.util.ConnectMySqlSource;
import test.test02.util.SourceVo;

import java.util.Properties;
import java.util.Random;

/**  process练习
 * @ClassName test0201
 * @Description
 * @Author wanghao
 * @Date 2021/1/11 13:35
 * @Version 1.0
 */
public class TestProcessProducer {
    public static void main(String[] args) {
        for(int i=0;i<=1000;i++){
            Random random=new Random();
            int intitem= random.nextInt(1000);
            String numstr=String.valueOf(intitem%10);
            TestProcessProducer testProcessProducer= new TestProcessProducer();
            testProcessProducer.send(numstr);
            System.out.println("已发送数据:"+numstr);

        }
    }

    /**发送方法
     * 发送消息配置
     * @param value
     */
    public void send(String value){
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");//xxx服务器ip
        props.put("acks", "all");//所有follower都响应了才认为消息提交成功，即"committed"
        props.put("retries", 0);//retries = MAX 无限重试，直到你意识到出现了问题:)
        //props.put("batch.size", 16384);//producer将试图批处理消息记录，以减少请求次数.默认的批量处理消息字节数
        //batch.size当批量的数据大小达到设定值后，就会立即发送，不顾下面的linger.ms
        props.put("linger.ms", 1);//延迟1ms发送，这项设置将通过增加小的延迟来完成--即，不是立即发送一条记录，producer将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理
        // props.put("buffer.memory", 33554432);//producer可以用来缓存数据的内存大小。
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        final KafkaProducer<String, String> producer;
        final  String TOPIC = "process_test1";
        producer = new KafkaProducer<String, String>(props);
        producer.send(new ProducerRecord<String, String>(TOPIC,value));
    }
}

