package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.common.GmallConstant;
import com.atguigu.gmall.realtime.app.utils.MyKafkaUtil;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 交易域：加购事实表
 */

//数据流:web/app -> Mysql -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序:Mock -> Mysql -> maxwell.sh -> Kafka(ZK) -> Dwd03_TradeCartAdd -> Kafka(ZK)
public class Dwd03_TradeCartAdd {

    public static void main(String[] args) {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.1 CK
        //        env.enableCheckpointing(10 * 60000);
        //        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //        checkpointConfig.setCheckpointTimeout(5 * 60000);
        //        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/flink/ck");
        //        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //
        //        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

        //TODO 2.读取 topic_db主题数据创建动态表
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_cart_add_1030"));

        //TODO 3.过滤加购数据
        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['cart_price'] cart_price,\n" +
                "    if(`type`='insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num,\n" +
                "    `data`['sku_name'] sku_name,\n" +
                "    `data`['is_checked'] is_checked,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['operate_time'] operate_time,\n" +
                "    `data`['source_type'] source_type,\n" +
                "    `data`['source_id'] source_id\n" +
                "from topic_db\n" +
                "where `database` = 'gmall-flink'\n" +
                "and `table` = 'cart_info'\n" +
                "and (`type` = 'insert' or (`type` = 'update' and `old`['sku_num'] is not null and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))");
        //tableEnv.createTemporaryView("result_table", resultTable);

        //TODO 4.构建DWD层加购主题

//        System.out.println("" +
//                "create table dwd_cart_info(\n" +
//                "    `id` string,\n" +
//                "    `user_id` string,\n" +
//                "    `sku_id` string,\n" +
//                "    `cart_price` string,\n" +
//                "    `sku_num` string,\n" +
//                "    `sku_name` string,\n" +
//                "    `is_checked` string,\n" +
//                "    `create_time` string,\n" +
//                "    `operate_time` string,\n" +
//                "    `source_type` string,\n" +
//                "    `source_id` string\n" +
//                ")" + MyKafkaUtil.getKafkaDDL(GmallConstant.TOPIC_DWD_TRADE_CART_ADD,GmallConstant.TOPIC_DWD_TRADE_CART_ADD));
        tableEnv.executeSql("" +
                "create table dwd_cart_info(\n" +
                "    `id` string,\n" +
                "    `user_id` string,\n" +
                "    `sku_id` string,\n" +
                "    `cart_price` string,\n" +
                "    `sku_num` string,\n" +
                "    `sku_name` string,\n" +
                "    `is_checked` string,\n" +
                "    `create_time` string,\n" +
                "    `operate_time` string,\n" +
                "    `source_type` string,\n" +
                "    `source_id` string\n" +
                ")" +MyKafkaUtil.getKafkaDDL(GmallConstant.TOPIC_DWD_TRADE_CART_ADD,GmallConstant.TOPIC_DWD_TRADE_CART_ADD));

//        System.out.println("" +
//                "create table dwd_cart_info(\n" +
//                "    `id` string,\n" +
//                "    `user_id` string,\n" +
//                "    `sku_id` string,\n" +
//                "    `cart_price` string,\n" +
//                "    `sku_num` string,\n" +
//                "    `sku_name` string,\n" +
//                "    `is_checked` string,\n" +
//                "    `create_time` string,\n" +
//                "    `operate_time` string,\n" +
//                "    `source_type` string,\n" +
//                "    `source_id` string\n" +
//                ")" + MyKafkaUtil.getKafkaSink(GmallConstant.TOPIC_DWD_TRADE_CART_ADD));

        //TODO 5.写出数据
        resultTable.insertInto("dwd_cart_info")
                .execute();

    }

}
