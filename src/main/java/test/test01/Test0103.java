package test.test01;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**数据迁移 表到表   db 到 db
 * @ClassName test0103
 * @Description TODO 数据迁移   db 到 db
 * @Author wanghao628   数据迁移 从test01表到test02表  批量
 * test01表  id   name  old
 * test02表 id  name_old
 * @Date 2021/1/8 16:14
 * @Version 1.0
 */
public class Test0103 {
    public static void main(String[] args) {

        try{
            String sql_select="select id,name,old,valuex from dbtest01 ";
            String sql_insert="insert into dbtest02 (id,name,value_name)values(?,?,?)";
            //创建数据源
            JDBCInputFormat jdbcInputFormat_Select = JDBCInputFormat.buildJDBCInputFormat()
                    .setDBUrl("jdbc:mysql://localhost:3306/test")
                    .setDrivername("com.mysql.jdbc.Driver")
                    .setUsername("root")
                    .setPassword("123456")
                    .setRowTypeInfo(new RowTypeInfo(Types.STRING, Types.STRING, Types.STRING))
                    .setQuery(sql_select)
                    .finish();
            JDBCOutputFormat jdbcOutputFormat_Insert = JDBCOutputFormat.buildJDBCOutputFormat()
                    .setDBUrl("jdbc:mysql://localhost:3306/test")
                    .setDrivername("com.mysql.jdbc.Driver")
                    .setUsername("root")
                    .setPassword("123456")
                    .setQuery(sql_insert)
                    .finish();
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            DataSet<Row> ds = env.createInput(jdbcInputFormat_Select);
            ds.map(new MapFunction<Row, Object>() {

                @Override
                public Row map(Row row) throws Exception {
                    String rowstr=row.toString();
                    String [] fieldArr=rowstr.split(",");
                    String fieldx=fieldArr[1];
                    String fieldy=fieldArr[2];
                    String field0=fieldArr[0];
                    String field1=fieldArr[1];
                    String fieldxy=fieldx+fieldy;
                    Row reslutRow=new Row(3);
                    reslutRow.setField(0,field0);
                    reslutRow.setField(1,field1);
                    reslutRow.setField(2,fieldxy);
                    return reslutRow;
                }
            });
            ds.output(jdbcOutputFormat_Insert);
            env.setParallelism(1);
            env.execute();
        }catch (Exception e){
                e.printStackTrace();
        }


    }

}
