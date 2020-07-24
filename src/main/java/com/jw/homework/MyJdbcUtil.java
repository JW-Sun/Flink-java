package com.jw.homework;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class MyJdbcUtil {
    public static void main(String[] args) throws Exception {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://192.168.159.102:3306/test_mall?characterEncoding=utf-8&useSSL=false";
        String username = "root";
        String password = "Sjw199795!";
        Connection connection = null;
        Statement statement = null;
        String sql = "select * from `order`";

        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                System.out.println(resultSet.getInt("id") + " " +
                        resultSet.getInt("consumer_id") + " " +
                        resultSet.getInt("product_id") + " " +
                        resultSet.getInt("num"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}
