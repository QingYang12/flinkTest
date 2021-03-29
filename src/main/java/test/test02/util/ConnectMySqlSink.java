package test.test02.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName ConnectMySql
 * @Description TODO
 * @Author wanghao628
 * @Date 2021/2/4 19:47
 * @Version 1.0
 */
public class ConnectMySqlSink extends RichSinkFunction<JSONObject> {
    private static final Logger logger = LoggerFactory.getLogger(ConnectMySqlSink.class);
    private Connection connection;

    private PreparedStatement preparedStatement;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 加载JDBC驱动

        Class.forName("com.mysql.jdbc.Driver");

        // 获取数据库连接

        connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test", "root", "123456");//获取连接

        preparedStatement = connection.prepareStatement("insert into dbtest02 (id,name,value_name)values(?,?,?)");

        super.open(parameters);

    }

    @Override
    public void close()throws Exception {
        super.close();
        if(preparedStatement != null){

            preparedStatement.close();

        }

        if(connection != null){

            connection.close();

        }

        super.close();

    }
    @Override
    public void invoke(JSONObject s) throws Exception {

        try {
            preparedStatement.setString(1,s.getString("id"));
            preparedStatement.setString(2,s.getString("name"));
            preparedStatement.setString(3,s.getString("value_name"));
            preparedStatement.executeUpdate();

        }catch (Exception e){

            e.printStackTrace();

        }

    }


}
