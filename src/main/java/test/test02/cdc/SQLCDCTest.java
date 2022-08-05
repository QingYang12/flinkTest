package test.test02.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test.test02.util.ConnectMySqlSource;
import test.test02.util.SourceVo;

import java.util.Properties;

/**flink SQL CDC 测试      mysql增量   到kafka   topic:  cdc_sql_test
 *
 */
public class SQLCDCTest {
    public static void main(String[] args) {
        try {
            final Logger LOG = LoggerFactory.getLogger(CDCTest.class);

            Properties properties = new Properties();
            properties.put("group.id", "flink-kafka-connector");
            properties.put("bootstrap.servers", "127.0.0.1:9092");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            //1.创建执行环境
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //2.flinkcdc 做断点续传,需要将flinkcdc读取binlog的位置信息以状态方式保存在checkpoint中即可.

            //(1)开启checkpoint 每隔5s 执行一次ck 指定ck的一致性语义
            env.setParallelism(1);
            /*env.enableCheckpointing(5000L);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

            //3.设置任务关闭后,保存最后后一次ck数据.
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000L));

            env.setStateBackend(new FsStateBackend("hdfs://192.168.1.204:9000/flinkCDC"));

            //4.设置访问HDFS的用户名
            System.setProperty("HADOOP_USER_NAME","root");*/
            //5.创建Sources数据源
            Properties prop = new Properties();
            prop.setProperty("scan.startup.mode","initial");  //"scan.startup.mode","initial" 三种要补充解释下

            DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                    .hostname("127.0.0.1")
                    .port(3306)
                    .username("root")
                    .password("123456")
                    .tableList("ssm.order") //这里的表名称,书写格式:db.table  不然会报错
                    .databaseList("ssm")
                    .debeziumProperties(prop)
                    .deserializer(new StringDebeziumDeserializationSchema())
                    .build();

            DataStreamSource<String> source =  env.addSource(mysqlSource);
            source.map(new MapFunction<String, String>() {
                @Override
                public String map(String sourceVo) throws Exception {
                    String sourceVoStr= JSON.toJSON(sourceVo).toString();
                    System.out.println("已读取 并发送到db_kafka_topic：" + sourceVoStr);
                    LOG.info("已读取 并发送到db_kafka_topic：" + sourceVoStr);
                    return sourceVoStr;
                }
            }).addSink((SinkFunction<String>) new FlinkKafkaProducer011<String>("cdc_test", new SimpleStringSchema(), properties));
            env.execute("CDCTest db to kafka");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
