package com.atguigu.gmall.realtime.dwd;

import com.atguigu.gmall.realtime.common.base.BaseSQL;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: DwdTradeOrderCancelDetail
 * Package: com.atguigu.gmall.realtime.dwd
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/22 17:45
 * @Version 1.0
 */
public class DwdTradeOrderCancelDetail extends BaseSQL {
    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(
                1007,
                4,
                null
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {

        //TODO 设置状态的保留时间[传输的延迟 + 业务上的滞后关系]
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15 * 60 + 5));

        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
        readOrderCancel(tableEnv);
        readOrderInfoDwd(tableEnv);


        //TODO 关联上述4张表
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id," +  // 年月日
                        "od.create_time," +
                        "oc.operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts " +
                        "from dwd_trade_order_detail od " +
                        " left join order_cancel oc on oc.id=od.id " +
                        "");
        tableEnv.createTemporaryView("result", result);

//        result.execute().print();

        //TODO 将关联的结果写到Kafka主题
        //创建动态表和要写入的主题进行映射
        tableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_ORDER_CANCEL + "(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "operate_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint," +
                        "primary key(id) not enforced " +
                        ")" + FlinkSQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));
        tableEnv.executeSql("insert into " + Constant.TOPIC_DWD_TRADE_ORDER_CANCEL + " select * from `result`");
    }

    private static void readOrderCancel(StreamTableEnvironment tableEnv) {
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['operate_time'] operate_time, " +
                " `ts` " +
                "from topic_db " +
                "where `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status']='1001' " +
                "and `data`['order_status']='1003' ");
        tableEnv.createTemporaryView("order_cancel", orderCancel);
//        orderCancel.execute().print();
    }

    private static void readOrderInfoDwd(StreamTableEnvironment tableEnv) {
        //TODO  从 `dwd_trade_order_detail` 过滤出订单数据

        String sql = "" +
                "create Table    dwd_trade_order_detail(\n" +
                "id  string,\n" +
                "order_id    string,\n" +
                "user_id string,\n" +
                "sku_id  string,\n" +
                "sku_name    string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id    string,\n" +
                "coupon_id   string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "sku_num string,\n" +
                "split_original_amount   string,\n" +
                "split_activity_amount   string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount  string,\n" +
                "ts  bigint\n" +
                ")" + FlinkSQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        tableEnv.executeSql(sql);

    }


}
