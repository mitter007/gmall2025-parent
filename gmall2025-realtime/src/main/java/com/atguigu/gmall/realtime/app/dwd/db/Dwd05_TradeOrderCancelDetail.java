package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
/**
 * 交易域取消订单事务事实表
 */
public class Dwd05_TradeOrderCancelDetail {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.4 设置状态的TTL
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15 * 60 + 10));
        //TODO 2.检查点相关设置(略)
        //TODO 3.从kafka的topic_db主题中读取数据 创建动态表
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_trade_order_cancel_group"));
        //TODO 4.过滤出订单明细
        Table orderDetail = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['sku_name'] sku_name,\n" +
                "data['create_time'] create_time,\n" +
                "data['source_id'] source_id,\n" +
                "data['source_type'] source_type,\n" +
                "data['sku_num'] sku_num,\n" +
                "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                "cast(data['order_price'] as decimal(16,2)) as String) split_original_amount,\n" +
                "data['split_total_amount'] split_total_amount,\n" +
                "data['split_activity_amount'] split_activity_amount,\n" +
                "data['split_coupon_amount'] split_coupon_amount \n" +
                "from `topic_db` where `table` = 'order_detail'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        //TODO 5.过滤出订单
        Table orderCancelInfo = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['province_id'] province_id,\n" +
                "data['operate_time'] operate_time,\n" +
                "ts\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_info'\n" +
                "and `type` = 'update'\n" +
                "and `old`['order_status'] is not null\n" +
                "and data['order_status'] = '1003'");
        tableEnv.createTemporaryView("order_cancel_info", orderCancelInfo);

        //TODO 6.过滤出订单明细活动
        Table orderDetailActivity = tableEnv.sqlQuery("select \n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['activity_id'] activity_id,\n" +
                "data['activity_rule_id'] activity_rule_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_activity'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        //TODO 7.过滤出订单明细优惠券
        Table orderDetailCoupon = tableEnv.sqlQuery("select\n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['coupon_id'] coupon_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_coupon'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        //TODO 8.关联上述4张表
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "od.id,\n" +
                "od.order_id,\n" +
                "oci.user_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "oci.province_id,\n" +
                "act.activity_id,\n" +
                "act.activity_rule_id,\n" +
                "cou.coupon_id,\n" +
                "date_format(oci.operate_time, 'yyyy-MM-dd') operate_date_id,\n" +
                "oci.operate_time,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount,\n" +
                "oci.ts \n" +
                "from order_detail od \n" +
                "join order_cancel_info oci\n" +
                "on od.order_id = oci.id\n" +
                "left join order_detail_activity act\n" +
                "on od.id = act.order_detail_id\n" +
                "left join order_detail_coupon cou\n" +
                "on od.id = cou.order_detail_id");
        tableEnv.createTemporaryView("result_table", resultTable);

        //TODO 10.将关联的结果写到kafka主题中
        //10.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("create table dwd_trade_cancel_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "date_id string,\n" +
                "cancel_time string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "ts string,\n" +
                "primary key(id) not enforced" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_cancel_detail"));


        //10.2 写入
        tableEnv.executeSql("insert into dwd_trade_cancel_detail select * from result_table");
    }
}
