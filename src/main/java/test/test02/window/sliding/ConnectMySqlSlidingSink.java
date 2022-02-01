package test.test02.window.sliding;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;
import java.util.Date;

/**
 * @ClassName ConnectMySql
 * @Description TODO
 * @Author wanghao
 * @Date 2021/2/4 19:47
 * @Version 1.0
 */
public class ConnectMySqlSlidingSink extends RichSinkFunction<JSONObject> {
    private static final Logger logger = LoggerFactory.getLogger(ConnectMySqlSlidingSink.class);
    private Connection connection;

    private PreparedStatement preparedStatementQ;
    private PreparedStatement preparedStatementI;
    private PreparedStatement preparedStatementU;
    private PreparedStatement preparedStatementD;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 加载JDBC驱动

        Class.forName("com.mysql.cj.jdbc.Driver");

        // 获取数据库连接

        connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test?characterEncoding=utf-8&useSSL=false", "root", "12345678");//获取连接

        preparedStatementQ = connection.prepareStatement("select * from usercount where user=?");
        preparedStatementI = connection.prepareStatement("insert into usercount (user,count,time)values(?,?,?)");
        preparedStatementU = connection.prepareStatement("update usercount  set count=? where user=? and time=?");
        preparedStatementD = connection.prepareStatement("delete from usercount uc where uc.time<=?");
        super.open(parameters);

    }

    @Override
    public void close()throws Exception {
        super.close();
        if(preparedStatementQ != null){

            preparedStatementQ.close();

        }

        if(connection != null){

            connection.close();

        }

        super.close();

    }
    @Override
    public void invoke(JSONObject s) throws Exception {

        try {
            SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
            Date date =sdf.parse(s.getString("time"));
            Calendar newTime = Calendar.getInstance(TimeZone.getTimeZone("GMT+08:00"));
            newTime.setTime(date);
            newTime.add(Calendar.SECOND,-10);//日期加10秒
            Date dt1=newTime.getTime();
            System.out.println(s.getString("time"));
            System.out.println(sdf.format(date));
            System.out.println(sdf.format(dt1));
            Timestamp datesnew =new  Timestamp(dt1.getTime());
            String datenewstr=sdf.format(datesnew);
            System.out.println(datenewstr);
            preparedStatementD.setTimestamp(1,  datesnew);
            preparedStatementD.executeUpdate();
            preparedStatementQ.setString(1,s.getString("user"));
            ResultSet resultSet = preparedStatementQ.executeQuery();
            List<HashMap> arr=Lists.newArrayList();
            while(resultSet.next()){
                HashMap item=new HashMap();
                item.put("user",resultSet.getString("user"));
                item.put("count",resultSet.getString("count"));
                arr.add(item);
            }
            if(!CollectionUtils.isEmpty(arr)){//
                preparedStatementU.setString(2,s.getString("user"));
                HashMap sResultSetItem= (HashMap)arr.get(0);
                Integer before=Integer.valueOf((String) sResultSetItem.get("count"));
                Integer after=Integer.valueOf(s.getString("count"));

                preparedStatementU.setString(1,String.valueOf(after));
                preparedStatementU.setTimestamp(3,datesnew);
                preparedStatementU.executeUpdate();
            }else{
                preparedStatementI.setString(1,s.getString("user"));
                preparedStatementI.setString(2,s.getString("count"));
                preparedStatementI.setTimestamp(3,datesnew);
                preparedStatementI.executeUpdate();
            }



        }catch (Exception e){

            e.printStackTrace();

        }

    }


}
