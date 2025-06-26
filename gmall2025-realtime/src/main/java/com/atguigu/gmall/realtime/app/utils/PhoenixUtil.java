package com.atguigu.gmall.realtime.app.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 操作phoenix的工具类
 */
public class PhoenixUtil {
    public static void executeSql(String sql) {
        // 1.添加链接
        String url = "jdbc:phoenix:hadoop202,hadoop203,hadoop204:2181";

        // 2. 创建配置
        // 没有需要添加的必要配置  因为Phoenix没有账号密码
        Properties properties = new Properties();
        Connection conn = null;

        try {
            // 3. 获取连接
            conn = DriverManager.getConnection(url, properties);
        } catch (SQLException e) {
            System.out.println("出错了");
            throw new RuntimeException(e);
        }
        PreparedStatement ps = null;
        try {
            conn = DruidDSUtil.getConnection();
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //释放资源
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
