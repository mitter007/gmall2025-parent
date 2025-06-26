package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.common.GmallConstant;
import com.atguigu.gmall.realtime.app.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;


/**
 * 第4章 交易域下单事务事实表
 */
//数据流:web/app -> Mysql -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序:Mock -> Mysql -> maxwell.sh -> Kafka(ZK) -> Dwd04_TradeOrderDetail -> Kafka(ZK)
public class Dwd04_TradeOrderDetail {

    public static void main(String[] args) {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置状态过期时间
//        tableEnv.getConfig().set("table.exec.state.ttl", "5s");
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        //1.1 CK
        //        env.enableCheckpointing(10 * 60000);
        //        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //        checkpointConfig.setCheckpointTimeout(5 * 60000);
        //        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/flink/ck");
        //        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //
        //        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

        //TODO 2.读取 topic_db 主题数据
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_order_1030"));

        //TODO 3.过滤出订单明细数据
        Table orderDetailTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['sku_name'] sku_name,\n" +
                "    `data`['order_price'] order_price,\n" +
                "    `data`['sku_num'] sku_num,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['source_type'] source_type,\n" +
                "    `data`['source_id'] source_id,\n" +
                "    `data`['split_total_amount'] split_total_amount,\n" +
                "    `data`['split_activity_amount'] split_activity_amount,\n" +
                "    `data`['split_coupon_amount'] split_coupon_amount\n" +
                "from topic_db\n" +
                "where `database` = 'gmall-flink'\n" +
                "and `table` = 'order_detail'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail", orderDetailTable);

        //TODO 4.过滤出订单数据
        Table orderInfoTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['consignee'] consignee,\n" +
                "    `data`['consignee_tel'] consignee_tel,\n" +
                "    `data`['total_amount'] total_amount,\n" +
                "    `data`['order_status'] order_status,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['delivery_address'] delivery_address,\n" +
                "    `data`['order_comment'] order_comment,\n" +
                "    `data`['out_trade_no'] out_trade_no,\n" +
                "    `data`['trade_body'] trade_body,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['expire_time'] expire_time,\n" +
                "    `data`['process_status'] process_status,\n" +
                "    `data`['tracking_no'] tracking_no,\n" +
                "    `data`['parent_order_id'] parent_order_id,\n" +
                "    `data`['province_id'] province_id,\n" +
                "    `data`['activity_reduce_amount'] activity_reduce_amount,\n" +
                "    `data`['coupon_reduce_amount'] coupon_reduce_amount,\n" +
                "    `data`['original_total_amount'] original_total_amount,\n" +
                "    `data`['feight_fee'] feight_fee,\n" +
                "    `data`['feight_fee_reduce'] feight_fee_reduce\n" +
                "from topic_db\n" +
                "where `database` = 'gmall-flink'\n" +
                "and `table` = 'order_info'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_info", orderInfoTable);

        //TODO 5.过滤出订单明细活动数据
        Table orderActivityTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['order_detail_id'] order_detail_id,\n" +
                "    `data`['activity_id'] activity_id,\n" +
                "    `data`['activity_rule_id'] activity_rule_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['create_time'] create_time\n" +
                "from topic_db\n" +
                "where `database` = 'gmall-flink'\n" +
                "and `table` = 'order_detail_activity'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_activity", orderActivityTable);

        //TODO 6.过滤出订单明细优惠券数据
        Table orderCouponTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['order_detail_id'] order_detail_id,\n" +
                "    `data`['coupon_id'] coupon_id,\n" +
                "    `data`['coupon_use_id'] coupon_use_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['create_time'] create_time\n" +
                "from topic_db\n" +
                "where `database` = 'gmall-flink'\n" +
                "and `table` = 'order_detail_coupon'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_coupon", orderCouponTable);

        //TODO 7.四表关联
        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    od.id,\n" +
                "    od.order_id,\n" +
                "    od.sku_id,\n" +
                "    od.sku_name,\n" +
                "    od.order_price,\n" +
                "    od.sku_num,\n" +
                "    od.create_time,\n" +
                "    od.source_type,\n" +
                "    od.source_id,\n" +
                "    od.split_total_amount,\n" +
                "    od.split_activity_amount,\n" +
                "    od.split_coupon_amount,\n" +
                "    oi.consignee,\n" +
                "    oi.consignee_tel,\n" +
                "    oi.total_amount,\n" +
                "    oi.order_status,\n" +
                "    oi.user_id,\n" +
                "    oi.delivery_address,\n" +
                "    oi.order_comment,\n" +
                "    oi.out_trade_no,\n" +
                "    oi.trade_body,\n" +
                "    oi.process_status,\n" +
                "    oi.tracking_no,\n" +
                "    oi.parent_order_id,\n" +
                "    oi.province_id,\n" +
                "    oi.activity_reduce_amount,\n" +
                "    oi.coupon_reduce_amount,\n" +
                "    oi.original_total_amount,\n" +
                "    oi.feight_fee,\n" +
                "    oi.feight_fee_reduce,\n" +
                "    oa.activity_id,\n" +
                "    oa.activity_rule_id,\n" +
                "    oc.coupon_id,\n" +
                "    oc.coupon_use_id\n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id=oi.id\n" +
                "left join order_activity oa\n" +
                "on od.id = oa.order_detail_id\n" +
                "left join order_coupon oc\n" +
                "on od.id = oc.order_detail_id");

        //TODO 8.构建DWD 层Kafka主题
        tableEnv.executeSql("" +
                "create table dwd_order_detail(\n" +
                "    `id` string,\n" +
                "    `order_id` string,\n" +
                "    `sku_id` string,\n" +
                "    `sku_name` string,\n" +
                "    `order_price` string,\n" +
                "    `sku_num` string,\n" +
                "    `create_time` string,\n" +
                "    `source_type` string,\n" +
                "    `source_id` string,\n" +
                "    `split_total_amount` string,\n" +
                "    `split_activity_amount` string,\n" +
                "    `split_coupon_amount` string,\n" +
                "    `consignee` string,\n" +
                "    `consignee_tel` string,\n" +
                "    `total_amount` string,\n" +
                "    `order_status` string,\n" +
                "    `user_id` string,\n" +
                "    `delivery_address` string,\n" +
                "    `order_comment` string,\n" +
                "    `out_trade_no` string,\n" +
                "    `trade_body` string,\n" +
                "    `process_status` string,\n" +
                "    `tracking_no` string,\n" +
                "    `parent_order_id` string,\n" +
                "    `province_id` string,\n" +
                "    `activity_reduce_amount` string,\n" +
                "    `coupon_reduce_amount` string,\n" +
                "    `original_total_amount` string,\n" +
                "    `feight_fee` string,\n" +
                "    `feight_fee_reduce` string,\n" +
                "    `activity_id` string,\n" +
                "    `activity_rule_id` string,\n" +
                "    `coupon_id` string,\n" +
                "    `coupon_use_id` string,\n" +
                "    primary key(id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL(GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        //TODO 9.写出数据
        resultTable.insertInto("dwd_order_detail")
                .execute();

    }

}
