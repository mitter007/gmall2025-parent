package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;

/**
 * ClassName: SQLUtil
 * Package: com.atguigu.gmall.realtime.common.util
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/22 11:14
 * @Version 1.0
 */
public class FlinkSQLUtil {
    public static String getKafkaDDL(String topic, String groupId) {
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = 'hadoop202:9092,hadoop203:9092,hadoop204:9092',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";

    }

    public static String getHBaseDDL(String tableName) {
        return " WITH (\n" +
                "  'connector' = 'hbase-2.2',\n" +
                "  'zookeeper.quorum' = 'hadoop202:2181,hadoop203:2181,hadoop204:2181',\n" +
                "  'table-name' = '" + Constant.HBASE_NAMESPACE + ":" + tableName + "',\n" +
//                下面这几行代码是什么意思
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";

/*
        'lookup.async' = 'true'	开启异步查询	false	Flink 查询 HBase 时，采用异步方式，提升吞吐、减少阻塞。建议开启。
        'lookup.cache' = 'PARTIAL'	开启部分缓存	NONE	表示使用部分缓存模式，缓存命中则返回结果，缓存未命中才去查 HBase
        'lookup.partial-cache.max-rows' = '500'	缓存最大行数	无	最多缓存 500 条 HBase 查询结果，超出时会触发 LRU 淘汰
        'lookup.partial-cache.expire-after-write' = '1 hour'	缓存写入后过期时间	无	缓存写入后，1 小时内有效
        'lookup.partial-cache.expire-after-access' = '1 hour'	缓存访问后过期时间	无	如果缓存 1 小时内没被访问，则过期移除
        */
    }

    public static String getUpsertKafkaDDL(String topic) {
        return " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.json.ignore-parse-errors' = 'true'," +
                "  'value.json.ignore-parse-errors' = 'true'," +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

}
