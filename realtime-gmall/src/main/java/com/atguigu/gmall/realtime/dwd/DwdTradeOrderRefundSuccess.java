package com.atguigu.gmall.realtime.dwd;

import com.atguigu.gmall.realtime.common.base.BaseSQL;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: DwdTradeOrderRefund
 * Package: com.atguigu.gmall.realtime.dwd
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/24 9:13
 * @Version 1.0
 */
public class DwdTradeOrderRefundSuccess extends BaseSQL {
    public static void main(String[] args) {
        new DwdTradeOrderRefundSuccess().start(
                1010,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_REFUND
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
//        一次退款支付操作成功时，退款表refund_payment、订单表order_info和退单表order_refund_info 的对应数据会发生修改，
//        几张表之间不存在业务时间上的滞后。与字典表的关联分析同上，不再赘述。因而，仅考虑可能的数据乱序即可。将ttl设置为5s。
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
//        TODO 创建表源
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
        readBaseDic(tableEnv);
        readOrderRefundInfoDwd(tableEnv);
        readRefundPayDwd(tableEnv);
        readOrderInfoDwd(tableEnv);

        // 6. 4 张表的 join
        Table result = tableEnv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "date_format(rp.callback_time,'yyyy-MM-dd') date_id," +
                        "rp.payment_type," +
                        "dic.info.dic_name payment_type_name," +
                        "rp.callback_time," +
                        "ori.refund_num," +
                        "rp.total_amount," +
                        "rp.ts " +
                        "from refund_payment rp " +
                        "join order_refund_info ori " +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        "join order_info oi " +
                        "on rp.order_id=oi.id " +
                        "join base_dic for system_time as of rp.pt as dic " +
                        "on rp.payment_type=dic.dic_code ");
        tableEnv.createTemporaryView("result_table", result);
        tableEnv.executeSql("create table dwd_trade_refund_payment_success(" +
                "         id string,\n" +
                "         user_id    string,\n" +
                "         order_id   string,\n" +
                "         sku_id string,\n" +
                "         province_id    string,\n" +
                "         date_id    string,\n" +
                "         payment_type_code    string,\n" +
                "         payment_type_name    string,\n" +
                "         callback_time string, \n" +
                "         refund_num string," +
                "         refund_amount string," +
                "         ts bigint," +
                "         PRIMARY KEY (id) NOT ENFORCED " +
                ")" + FlinkSQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));
        tableEnv.executeSql("insert into " + Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS + " select * from `result_table`");


    }

    private static void readRefundPayDwd(StreamTableEnvironment tableEnv) {
        //TODO 过滤出退款明细数据
        String sql = "select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['out_trade_no'] out_trade_no,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['payment_type'] payment_type,\n" +
                "    `data`['trade_no'] trade_no,\n" +
                "    `data`['total_amount'] total_amount,\n" +
                "    `data`['subject'] subject,\n" +
                "    `data`['refund_status'] refund_status,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['callback_time'] callback_time,\n" +
                "    `data`['callback_content'] callback_content,\n" +
                "    `data`['operate_time'] operate_time,\n" +
                "   `pt`," +
                "   ts, " +
                "   et " +
                "from topic_db where `table`='refund_payment' " +
                "and" +
                "    type = 'update'  and " +
                "    `data`['refund_status'] = '1602' and `old`['refund_status'] is not null";
        Table table = tableEnv.sqlQuery(sql);
//        table.execute().print();
        tableEnv.createTemporaryView("refund_payment", table);


    }


    private static void readOrderInfoDwd(StreamTableEnvironment tableEnv) {
        //TODO  订单详情表

        String sql = "select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['consignee'] consignee,\n" +
                "    `data`['consignee_tel'] consignee_tel,\n" +
                "    `data`['total_amount'] total_amount,\n" +
                "    `data`['order_status'] order_status,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['payment_way'] payment_way,\n" +
                "    `data`['delivery_address'] delivery_address,\n" +
                "    `data`['order_comment'] order_comment,\n" +
                "    `data`['out_trade_no'] out_trade_no,\n" +
                "    `data`['trade_body'] trade_body,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['operate_time'] operate_time,\n" +
                "    `data`['expire_time'] expire_time,\n" +
                "    `data`['process_status'] process_status,\n" +
                "    `data`['tracking_no'] tracking_no,\n" +
                "    `data`['parent_order_id'] parent_order_id,\n" +
                "    `data`['img_url'] img_url,\n" +
                "    `data`['province_id'] province_id,\n" +
                "    `data`['activity_reduce_amount'] activity_reduce_amount,\n" +
                "    `data`['coupon_reduce_amount'] coupon_reduce_amount,\n" +
                "    `data`['original_total_amount'] original_total_amount,\n" +
                "    `data`['feight_fee'] feight_fee,\n" +
                "    `data`['feight_fee_reduce'] feight_fee_reduce,\n" +
                "   `pt`," +
                "   ts, " +
                "   et " +
                "from topic_db where `table`='order_info' \n" +
                "and \n" +
                "`type` = 'update' and \n" +
                "`data`['order_status'] = '1006'";
        Table table = tableEnv.sqlQuery(sql);

//        table.execute().print();
        tableEnv.createTemporaryView("order_info", table);


    }

    private static void readOrderRefundInfoDwd(StreamTableEnvironment tableEnv) {
        //TODO 过滤出退款明细数据
        String sql = "select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['refund_type'] refund_type,\n" +
                "    `data`['refund_num'] refund_num,\n" +
                "    `data`['refund_amount'] refund_amount,\n" +
                "    `data`['refund_reason_type'] refund_reason_type,\n" +
                "    `data`['refund_reason_txt'] refund_reason_txt,\n" +
                "    `data`['refund_status'] refund_status,\n" +
                "    `data`['create_time'] create_time,\n" +
                "   `pt`," +
                "   ts, " +
                "   et " +
                "from topic_db where `table`='order_refund_info' " +
                "and" +
                "    type = 'update'  and " +
                "   `data`['refund_status'] = '0705'";
        Table table = tableEnv.sqlQuery(sql);
//                table.execute().print();
        tableEnv.createTemporaryView("order_refund_info", table);

    }
}
