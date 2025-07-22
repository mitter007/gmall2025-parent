package com.atguigu.gmall.realtime.dwd;

import com.atguigu.gmall.realtime.common.base.BaseSQL;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwdTradeOrderDetail
 * Package: com.atguigu.gmall.realtime.dwd
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/22 16:25
 * @Version 1.0
 */
public class DwdTradeOrderDetail extends BaseSQL {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(
                1006,
                4,
                null
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        readOrderDetail(tableEnv);
        readOrderInfo(tableEnv);
        readOrderDetailActivity(tableEnv);
        readOrderDetailCoupon(tableEnv);
        //TODO 关联上述4张表
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id," +
                        "od.order_id," +
                        "oi.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "oi.province_id," +
                        "act.activity_id," +
                        "act.activity_rule_id," +
                        "cou.coupon_id," +
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id," +  // 年月日
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts " +
                        "from order_detail od " +
                        "join order_info oi on od.order_id=oi.id " +
                        "left join order_detail_activity act " +
                        "on od.id=act.order_detail_id " +
                        "left join order_detail_coupon cou " +
                        "on od.id=cou.order_detail_id ");
        result.execute().print();
    }

    private static void readOrderDetail(StreamTableEnvironment tableEnv) {
        String sql = "select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['sku_name'] sku_name,\n" +
                "    `data`['order_price'] order_price,\n" +
                "    `data`['sku_num'] sku_num,\n" +
                "    `data`['create_time'] create_time,\n" +
                "   `data`['source_id'] source_id,\n" +
                "   `data`['source_type'] source_type,\n" +
                "   `data`['sku_num'] sku_num,\n" +
                "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                "   cast(data['order_price'] as decimal(16,2)) as String) split_original_amount," + // 分摊原始总金额
                "   `data`['split_total_amount'] split_total_amount,\n" +  // 分摊总金额
                "   `data`['split_activity_amount'] split_activity_amount,\n" + // 分摊活动金额
                "   `data`['split_coupon_amount'] split_coupon_amount,\n" + // 分摊的优惠券金额
                "    ts,\n" +
                "    pt\n" +
                "from topic_db where `table`='order_detail' and" +
                "    type = 'insert'\n";
        Table table = tableEnv.sqlQuery(sql);
//        table.execute().print();
        tableEnv.createTemporaryView("order_detail", table);
    }

    private static void readOrderInfo(StreamTableEnvironment tableEnv) {
        //TODO 过滤出订单数据
        String sql = "select \n" +
                "    `type` type,\n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['province_id'] province_id,\n" +
                "    `old`['order_status'] order_status,\n" +
                "    ts,\n" +
                "    pt\n" +
                "from topic_db where `table`='order_info' ";
        Table table = tableEnv.sqlQuery(sql);
        tableEnv.createTemporaryView("order_info", table);
//        tableEnv.executeSql(sql).print();

    }

    private static void readOrderDetailActivity(StreamTableEnvironment tableEnv) {
        //TODO 过滤出明细活动数据
        String sql = "select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['order_detail_id'] order_detail_id,\n" +
                "    `data`['activity_id'] activity_id,\n" +
                "    `data`['activity_rule_id'] activity_rule_id,\n" +
                "    ts,\n" +
                "    pt\n" +
                "from topic_db where `table`='order_detail_activity' and" +
                "    type = 'insert'\n";
        Table table = tableEnv.sqlQuery(sql);
        tableEnv.createTemporaryView("order_detail_activity", table);
//        tableEnv.executeSql(sql).print();
    }

    private static void readOrderDetailCoupon(StreamTableEnvironment tableEnv) {
        String sql = "select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['order_detail_id'] order_detail_id,\n" +
                "    `data`['coupon_id'] coupon_id,\n" +
                "    ts,\n" +
                "    pt\n" +
                "from topic_db where `table`='order_detail_coupon' and" +
                "    type = 'insert'\n";
        Table table = tableEnv.sqlQuery(sql);
        tableEnv.createTemporaryView("order_detail_coupon", table);

    }

}
