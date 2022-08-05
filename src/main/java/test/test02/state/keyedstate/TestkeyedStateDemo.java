package test.test02.state.keyedstate;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**  练习keyedStream
 * @author
 * @title: TestkeyedStateDemo
 * @projectName flinkTest
 * @description:
 * @date 2022/1/1011:04 上午
 */
public class TestkeyedStateDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> inputStream=env.fromElements(Tuple2.of(1L,4L),
                Tuple2.of(2L,3L),
                Tuple2.of(3L,1L),
                Tuple2.of(1L,2L),
                Tuple2.of(3L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(2L,2L),
                Tuple2.of(2L,9L));
        inputStream.keyBy(0).flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Object>() {

            private transient ValueState<Tuple2<Long,Long>> sum;
            @Override
            public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Object> collector) throws Exception {
                Tuple2<Long,Long> currentSum=sum.value();

                if(null==currentSum){
                    currentSum=Tuple2.of(0L,0L);
                }

                currentSum.f0+=1;

                currentSum.f1+=longLongTuple2.f1;

                sum.update(currentSum);
                if(currentSum.f0>=3){
                    collector.collect(Tuple2.of(longLongTuple2.f0,currentSum.f1/currentSum.f0));
                    sum.clear();
                }

            }
            @Override
            public void open(Configuration parameters) throws Exception{
                StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

                /**
                 * 注意这里仅仅用了状态，但是没有利用状态来容错
                 */
                ValueStateDescriptor<Tuple2<Long,Long>> descriptor=
                        new ValueStateDescriptor<>(
                                "avgState",
                                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})
                        );
        descriptor.enableTimeToLive(ttlConfig);

                sum=getRuntimeContext().getState(descriptor);

            }



        }).setParallelism(10).print();
        env.execute();




    }
}
