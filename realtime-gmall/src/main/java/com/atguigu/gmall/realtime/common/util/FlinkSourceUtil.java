package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.Data;

/**
 * ClassName: MysqlUtil
 * Package: com.atguigu.gmall.realtime.common.util
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/20 22:23
 * @Version 1.0
 */
public class FlinkSourceUtil {
    public static MySqlSource<String> getMysqlcdc(String database,String tableName) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(3306)
                .databaseList(database) // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList(database+"."+tableName) // set captured table
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        return mySqlSource;
    }

}
