package com.jw.homework;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;

public class MySourceOrderMysql extends RichSourceFunction<Order> {

    String driver = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://192.168.159.102:3306/test_mall?characterEncoding=utf-8&useSSL=false";
    String username = "root";
    String password = "Sjw199795!";
    Connection connection = null;
    Statement statement = null;
    PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "select * from `order`";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Order order = new Order(resultSet.getInt("id"),
                    resultSet.getInt("consumer_id"),
                    resultSet.getInt("product_id"),
                    resultSet.getInt("num"));
            ctx.collect(order);
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (ps != null) {
            ps.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    private Connection getConnection() {
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }
}
