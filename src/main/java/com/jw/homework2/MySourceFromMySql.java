package com.jw.homework2;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MySourceFromMySql extends RichSourceFunction<Top10Product> {

    String driver = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://192.168.159.102:3306/test_mall?characterEncoding=utf-8&useSSL=false";
    String username = "root";
    String password = "Sjw199795!";
    Connection connection = null;
    PreparedStatement ps = null;

    private Connection getConnection() {
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "SELECT od.product_id, p.NAME name, sum( od.num ) sum " +
                "FROM order_detail od " +
                "JOIN product p ON od.product_id = p.id " +
                "GROUP BY od.product_id, p.NAME " +
                "ORDER BY sum( od.num ) DESC LIMIT 10";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<Top10Product> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()) {
            Top10Product top10Product = new Top10Product(resultSet.getLong("product_id"),
                    resultSet.getString("name"),
                    resultSet.getLong("sum"));

            ctx.collect(top10Product);
        }
    }

    @Override
    public void close() throws Exception {
        if (ps != null) {
            ps.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void cancel() {

    }
}
