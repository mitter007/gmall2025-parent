package com.atguigu.gmall.realtime.app.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.atguigu.gmall.realtime.app.common.GmallConstant;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Desc: 获取数据库连接池对象
 */
public class DruidDSUtil {
    private static DruidDataSource druidDataSource;

    static {
        System.out.println("~~~获取Druid连接池对象~~~");
        // 创建连接池
        druidDataSource = new DruidDataSource();
        // 设置驱动全类名
        druidDataSource.setDriverClassName(GmallConstant.PHOENIX_DRIVER);
        // 设置连接 url
        druidDataSource.setUrl(GmallConstant.PHOENIX_URL);
        // 设置初始化连接池时池中连接的数量
        druidDataSource.setInitialSize(5);
        // 设置同时活跃的最大连接数
        druidDataSource.setMaxActive(20);
        // 设置空闲时的最小连接数，必须介于 0 和最大连接数之间，默认为 0
        druidDataSource.setMinIdle(5);
        // 设置没有空余连接时的等待时间，超时抛出异常，-1 表示一直等待
        druidDataSource.setMaxWait(-1);
        // 验证连接是否可用使用的 SQL 语句
        druidDataSource.setValidationQuery("select 1");
        // 指明连接是否被空闲连接回收器（如果有）进行检验，如果检测失败，则连接将被从池中去除
        // 注意，默认值为 true，如果没有设置 validationQuery，则报错
        // testWhileIdle is true, validationQuery not set
        druidDataSource.setTestWhileIdle(true);
        // 借出连接时，是否测试，设置为true,避免由于长链接失效导致的异常
        druidDataSource.setTestOnBorrow(true);
        // 归还连接时，是否测试
        druidDataSource.setTestOnReturn(false);
        // 设置空闲连接回收器每隔 30s 运行一次
        druidDataSource.setTimeBetweenEvictionRunsMillis(30 * 1000L);
        // 设置池中连接空闲 30min 被回收，默认值即为 30 min
        druidDataSource.setMinEvictableIdleTimeMillis(30 * 60 * 1000L);
    }

    public static Connection getConnection() throws SQLException {
        DruidPooledConnection conn = druidDataSource.getConnection();
        return conn;
    }
}
