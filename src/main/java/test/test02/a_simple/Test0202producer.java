package test.test02.a_simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**造 随机单词的数据  1000个 到test_word1
 * @ClassName test02producer
 * @Description TODO kafka 到 kafka producer
 * @Author wanghao
 * @Date 2021/1/11 13:39
 * @Version 1.0
 */
public class Test0202producer {
    public static void main(String[] args) {
        //a-z：97-122，A-Z：65-90，0-9：48-57   ascii
        for(int i=0;i<=1000;i++){
            Random random=new Random();
            char worditem1= (char) ((char)random.nextInt(122)%(122-97+1)+97);//a-z
            char worditem2= (char) ((char)random.nextInt(122)%(122-97+1)+97);//a-z
            char worditem3= (char) ((char)random.nextInt(122)%(122-97+1)+97);//a-z
            char worditem4= (char) ((char)random.nextInt(122)%(122-97+1)+97);//a-z
            String word_str=""+worditem1+worditem2+worditem3+worditem4;
            Test0202producer test02producer= new Test0202producer();
            test02producer.send(word_str);
            System.out.println("已发送数据:"+word_str);

        }
    }

    /**发送方法
     * 发送消息配置
     * @param value
     */
    void send(String value){
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
        final  String TOPIC = "test_word1";
        producer = new KafkaProducer<String, String>(props);
        producer.send(new ProducerRecord<String, String>(TOPIC,value));
    }
}
