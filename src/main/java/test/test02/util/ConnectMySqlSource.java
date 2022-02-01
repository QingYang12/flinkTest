package test.test02.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @ClassName ConnectMySql
 * @Description TODO
 * @Author wanghao
 * @Date 2021/2/4 19:47
 * @Version 1.0
 */
public class ConnectMySqlSource extends RichSourceFunction<SourceVo> {
    private static final Logger logger = LoggerFactory.getLogger(ConnectMySqlSource.class);
    private Connection connection = null;

    private PreparedStatement ps = null;
    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);

       // Class.forName("com.mysql.cj.jdbc.Driver");//加载数据库驱动

       // connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test?characterEncoding=utf-8&useSSL=false", "root", "12345678");//获取连接



        Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动

        connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test", "root", "123456");//获取连接

        ps = connection.prepareStatement("select id,name,old,valuex from dbtest01 ");

    }
    @Override
    public void run(SourceContext<SourceVo> ctx) throws Exception {

        try {

            ResultSet resultSet = ps.executeQuery();


            while (resultSet.next()) {

                SourceVo vo = new SourceVo();

                vo.setId(resultSet.getString("id"));
                vo.setName(resultSet.getString("name"));
                vo.setOld(resultSet.getString("old"));
                vo.setValuex(resultSet.getString("valuex"));
                ctx.collect(vo);

            }

        } catch (Exception e) {

            logger.error("runException:{}", e);

        }
    }
    @Override
    public void cancel() {

        try {

            super.close();

            if (connection != null) {

                connection.close();

            }

            if (ps != null) {

                ps.close();

            }

        } catch (Exception e) {

            logger.error("runException:{}", e);

        }

    }
}
