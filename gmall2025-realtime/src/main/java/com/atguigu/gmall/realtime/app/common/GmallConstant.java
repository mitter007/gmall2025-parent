package com.atguigu.gmall.realtime.app.common;

/**
 * Desc: 实时数仓系统常量类
 */
public class GmallConstant {
    // Phoenix库名
    public static final String PHOENIX_SCHEMA = "GMALL_REALTIME";
    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    // Phoenix连接参数
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop202,hadoop203,hadoop204:2181";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
}
