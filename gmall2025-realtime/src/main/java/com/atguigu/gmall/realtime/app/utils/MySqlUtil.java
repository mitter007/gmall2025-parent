package com.atguigu.gmall.realtime.app.utils;

/**
 * 操作mysql的工具类
 */
public class MySqlUtil {
    //获取从mysql中读取数据创建的动态表的建表语句
    public static String getBaseDicLookUpDDL() {
        return "CREATE TABLE base_dic (\n" +
            "  dic_code string,\n" +
            "  dic_name STRING,\n" +
            "  PRIMARY KEY (dic_code) NOT ENFORCED\n" +
            ")" + mysqlLookUpTableDDL("base_dic");
    }

    //获取jdbc的连接属性
    public static String mysqlLookUpTableDDL(String tableName) {
        return "WITH (\n" +
            "   'connector' = 'jdbc',\n" +
            "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
            "   'url' = 'jdbc:mysql://hadoop202:3306/gmall-flink?useSSL=false',\n" +
            "   'table-name' = '" + tableName + "',\n" +
            "   'username' = 'root',\n" +
            "   'password' = '000000',\n" +
            "   'lookup.cache' = 'PARTIAL',\n" +
            "   'lookup.partial-cache.max-rows' = '200',\n" +
            "   'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
            "   'lookup.partial-cache.expire-after-access' = '1 hour')";
    }
}
