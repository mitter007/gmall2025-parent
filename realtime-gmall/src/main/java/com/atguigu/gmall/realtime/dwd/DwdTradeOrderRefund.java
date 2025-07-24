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
public class DwdTradeOrderRefund extends BaseSQL {
    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(
                1009,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_REFUND
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
        readBaseDic(tableEnv);
        readOrderInfoDwd(tableEnv);
        readOrderRefundInfoDwd(tableEnv);
        // 4. join: 普通的和 lookup join
        Table result = tableEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(ri.create_time,'yyyy-MM-dd') date_id," +
                        "ri.create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts " +
                        "from order_refund_info ri " +
                        "join order_info oi " +
                        "on ri.order_id=oi.id " +
//                        在这里为什么连续join两次
                        "join base_dic for system_time as of ri.pt as dic1 " +
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.pt as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code ");
        tableEnv.createTemporaryView("result_table", result);
//        result.execute().print();
        tableEnv.executeSql("create table dwd_trade_order_refund(" +
                "         id string,\n" +
                "         user_id    string,\n" +
                "         order_id   string,\n" +
                "         sku_id string,\n" +
                "         province_id    string,\n" +
                "         date_id    string,\n" +
                "         create_time    string,\n" +
                "         refund_type_code string, \n" +
                "         refund_type_name string, \n" +
                "         refund_reason_type_code string, \n" +
                "         refund_reason_type_name string, \n" +
                "         refund_reason_txt  string,\n" +
                "         refund_num string,\n" +
                "         refund_amount  string,\n" +
                "         ts bigint," +
                "         PRIMARY KEY (id) NOT ENFORCED " +
                ")" + FlinkSQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));
        tableEnv.executeSql("insert into " + Constant.TOPIC_DWD_TRADE_ORDER_REFUND + " select * from `result_table`");


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
                "     `data`['create_time'] create_time,\n" +
                "   `pt`," +
                "   ts, " +
                "   et " +
                "from topic_db where `table`='order_refund_info' " +
                "and" +
                "    type = 'insert' ";
//                "    `data`['refund_status'] = '0705'";
        Table table = tableEnv.sqlQuery(sql);
//                table.execute().print();
        tableEnv.createTemporaryView("order_refund_info", table);

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
                "    type = 'update' and \n" +
                "    `data`['order_status'] = '1005' and  `old`['order_status'] = '1002'";
        Table table = tableEnv.sqlQuery(sql);

//        table.execute().print();
        tableEnv.createTemporaryView("order_info", table);



 /*
 		"id": 44030,
		"consignee": "殷露瑶",
		"consignee_tel": "13486535952",
		"total_amount": 300.0,
		"order_status": "1002",
		"user_id": 782,
		"payment_way": "3501",
		"delivery_address": null,
		"order_comment": null,
		"out_trade_no": "637771574221382",
		"trade_body": "香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 5号淡香水35ml等1件商品",
		"create_time": "2025-06-11 15:59:41",
		"operate_time": "2025-06-11 16:00:18",
		"expire_time": null,
		"process_status": null,
		"tracking_no": null,
		"parent_order_id": null,
		"img_url": null,
		"province_id": 18,
		"activity_reduce_amount": 0.0,
		"coupon_reduce_amount": 0.0,
		"original_total_amount": 300.0,
		"feight_fee": null,
		"feight_fee_reduce": null,
		"refundable_time": "2025-06-18 15:59:41"
  */

    }

}
