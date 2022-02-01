package test.test02.table.mqtodb;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author wanghao
 * @title: FlinkSql01Producer
 * @projectName flinkTest
 * @description: TODO
 * @date 2022/1/117:20 下午
 */
public class FlinkSql01Producer {
    static int index=0;
    public static void main(String[] args) {
        Timer t = new Timer();//创建1个定时器
        t.schedule(new TimerTask() {

            @Override
            public void run() {
                Random random=new Random();
                int itemid= random.nextInt(1000);
                int useritem= random.nextInt(1000);
                String behavior= "pv";
                int categoryid= random.nextInt(1000);
                Date date=new Date();
                SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
                FlinkSql01Producer flinkSql01Producer = new FlinkSql01Producer();
                JSONObject userItem=new JSONObject();
                userItem.put("user_id",String.valueOf(useritem));
                userItem.put("item_id",String.valueOf(itemid));
                userItem.put("category_id",String.valueOf(categoryid));
                userItem.put("behavior",String.valueOf(behavior));
                userItem.put("ts",sdf.format(date));
                String json =userItem.toJSONString();
                flinkSql01Producer.send(json);
                System.out.println("已发送数据:"+json);
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
        final  String TOPIC = "flink-sql-kafka";
        producer = new KafkaProducer<String, String>(props);
        producer.send(new ProducerRecord<String, String>(TOPIC,value));
    }
}
