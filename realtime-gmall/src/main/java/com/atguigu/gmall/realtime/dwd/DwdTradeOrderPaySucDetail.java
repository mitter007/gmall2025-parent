package com.atguigu.gmall.realtime.dwd;

import com.atguigu.gmall.realtime.common.base.BaseSQL;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwdTradeOrderPaySucDetail
 * Package: com.atguigu.gmall.realtime.dwd
 * Description:1）设置 ttl
 * 本节设置了事件时间，通过Interval Join实现了左右流的状态管理，无须设置ttl。
 *
 * @Author JWT
 * @Create 2025/7/22 22:55
 * @Version 1.0
 */

public class DwdTradeOrderPaySucDetail extends BaseSQL {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(
                1008,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
        readOrderInfoDwd(tableEnv);
//        TODO 从字典表中读取字典数据 创建动态表
        readBaseDic(tableEnv);
        readPaymentInfoDwd(tableEnv);

        //TODO 和字典进行关联---lookup join 和下单数据进行关联---IntervalJoin
//        5）关联上述三张表形成支付成功宽表，写入 Kafka 支付成功主题
//        支付成功业务过程的最细粒度为一个订单下一个sku的支付成功记录。从topic_db主题筛选的支付成功数据与字典表关联后粒度不变，为一个订单的支付成功记录，再关联订单明细表，
//        粒度与支付成功业务过程的最细粒度相同。
//（1）不是以流的形式存在，当主流数据到来后去MySQL中获取对应的维度数据即可，因此使用内连接即可。下文与字典表的关联同理，不再赘述。
//（2）通过Interval Join将下单明细表与上面的结果表关联。订单明细数据的产生必然早于支付数据，通常下单后最晚15min内完成支付，超时订单取消。此外，支付数据和订单明细数据位于两条流中，没有严格的先后关系，
// 当支付时间和下单时间非常接近时，可能由于网络问题导致支付数据先到，因此支付数据也要在状态中保留一段时间。本节要求订单明细数据的生成时间处于支付数据生成时间之前15min和之后5s范围内

        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "dic.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id=od.order_id " +
                        "and od.et >= pi.et - interval '30' minute " +
                        "and od.et <= pi.et + interval '5' second " +
//                （1）不是以流的形式存在，当主流数据到来后去MySQL中获取对应的维度数据即可，因此使用内连接即可。下文与字典表的关联同理，不再赘述。
                        "join base_dic for system_time as of pi.pt as dic " +
                        "on pi.payment_type=dic.dic_code ");
        tableEnv.createTemporaryView("result", result);

//        tableEnv.executeSql("select * from `result`").print();
        String sql = "create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + "(\n" +
                "order_detail_id string,\n" +
                "order_id    string,\n" +
                "user_id string,\n" +
                "sku_id  string,\n" +
                "sku_name    string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id    string,\n" +
                "coupon_id   string,\n" +
                "payment_type_code   string,\n" +
                "payment_type_name   string,\n" +
                "callback_time   string,\n" +
                "sku_num string,\n" +
                "split_original_amount   string,\n" +
                "split_activity_amount   string,\n" +
                "split_coupon_amount string,\n" +
                "split_payment_amount    string,\n" +
                "ts  bigint,\n" +
                "PRIMARY KEY (order_detail_id) NOT ENFORCED \n" +
//                kafkasink 不需要主键，upsertkafka才需要主键
//      ' with 'json' format doesn't support defining PRIMARY KEY constraint on the table, because it can't guarantee the semantic of primary key.
                ")\n" + FlinkSQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
        tableEnv.executeSql(sql);

        tableEnv.executeSql("insert into " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + " select * from `result`");



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
                "ts  bigint,\n" +
                "et as to_timestamp_ltz(ts, 0), " +
                "watermark for et as et - interval '3' second " +
                ")" + FlinkSQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        tableEnv.executeSql(sql);

    }

    private static void readPaymentInfoDwd(StreamTableEnvironment tableEnv) {
        //TODO 过滤出明细活动数据
        String sql = "select \n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['payment_type'] payment_type,\n" +
                "    `data`['payment_status'] payment_status,\n" +
                "   data['callback_time'] callback_time," +
                "   `pt`," +
                "   ts, " +
                "   et " +
                "from topic_db where `table`='payment_info' and" +
                "    type = 'update' and \n" +
                "    `data`['payment_status'] = '1602'";
        Table table = tableEnv.sqlQuery(sql);
        tableEnv.createTemporaryView("payment_info", table);

    }
}
