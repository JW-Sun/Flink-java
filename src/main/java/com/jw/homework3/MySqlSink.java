package com.jw.homework3;

import com.jw.homework3.entity.KfkSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySqlSink extends RichSinkFunction<KfkSource> {
    private String url = "jdbc:mysql://192.168.159.102:3306/test_flink_canal?useUnicode=true&characterEncoding=utf8&useSSL=false";
    private String username = "root";
    private String password = "Sjw199795!";

    private Connection connection = null;
    private PreparedStatement preparedStatement = null;

    // 新增
    private String insertSql = "insert into sink(userId, userName, userAge) values(?, ?, ?)";
    // 更新
    private String updateSql = "update sink set userName = ?, userAge = ? where userId = ?";
    // 删除
    private String deleteSql = "delete from sink where userId = ?";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection(url, username, password);
    }

    @Override
    public void invoke(KfkSource data, Context context) throws Exception {
        String type = data.getBase_info();
        System.out.println(data);
        if ("INSERT".equals(type)) {
            preparedStatement = connection.prepareStatement(insertSql);
            preparedStatement.setInt(1, data.getUserId());
            preparedStatement.setString(2, data.getUserName());
            preparedStatement.setInt(3, data.getUserAge());
            boolean pe = preparedStatement.execute();
            System.out.println("向目标数据库插入数据");
        } else if ("UPDATE".equals(type)) {
            preparedStatement = connection.prepareStatement(updateSql);
            preparedStatement.setString(1, data.getUserName());
            preparedStatement.setInt(2, data.getUserAge());
            preparedStatement.setInt(3, data.getUserId());
            boolean pe = preparedStatement.execute();
            System.out.println("向目标数据库更新数据");
        } else if ("DELETE".equals(type)) {
            preparedStatement = connection.prepareStatement(deleteSql);
            preparedStatement.setInt(1, data.getUserId());
            boolean pe = preparedStatement.execute();
            System.out.println("向目标数据库删除数据");
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
