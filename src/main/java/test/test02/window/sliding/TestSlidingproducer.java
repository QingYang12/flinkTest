package test.test02.window.sliding;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test.test02.Test0201producer;
import test.test02.util.ConnectMySqlSource;
import test.test02.util.SourceVo;

import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author
 * @title: Testproducer
 * @projectName flinkTest
 * @description: TODO
 * @date 2022/1/511:57 上午
 */
public class TestSlidingproducer {
    static  int index =0;//计算循环输出次数；
    public static void main(String[] args) {
        String[] users={"user1","user2","user3"};
        Random ran =new Random();
        Timer t = new Timer();//创建1个定时器
        t.schedule(new TimerTask() {

            @Override
            public void run() {
                index++;
                int x=ran.nextInt(3);
                String str=users[x];
                String value="{user:\""+str+"\",count:1}";
                send(JSON.toJSON(value).toString());
                if(index>=1000){//终止条件
                    t.cancel();
                }

            }
        }, 0, 1000);//// 首次运行,延迟0毫秒,循环间隔1000毫秒
    }


    /**发送方法
     * 发送消息配置
     * @param value
     */
   static void send(String value){
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
        final  String TOPIC = "test_sliding_window_1";
        producer = new KafkaProducer<String, String>(props);
        producer.send(new ProducerRecord<String, String>(TOPIC,value));
    }
}
