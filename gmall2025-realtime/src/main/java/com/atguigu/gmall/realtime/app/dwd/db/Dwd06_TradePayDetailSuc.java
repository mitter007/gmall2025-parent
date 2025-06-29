package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.utils.MyKafkaUtil;
import com.atguigu.gmall.realtime.app.utils.MySqlUtil;
import com.atguigu.gmall.realtime.app.utils.PhoenixUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


//数据流:web/app -> Mysql -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp(码表) -> Kafka(DWD)
//程  序:Mock -> Mysql -> maxwell.sh -> Kafka(ZK) -> Dwd04_TradeOrderDetail -> Kafka(ZK) -> Dwd06_TradePayDetailSuc(HBase/HDFS/ZK) -> Kafka(ZK)
public class Dwd06_TradePayDetailSuc {
    public static void main(String[] args) throws Exception {

        // TODO 1. 基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 指定表指定环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 检查点相关设置(略)

        // TODO 3. 读取 Kafka dwd_trade_order_detail 主题数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table dwd_trade_order_detail(\n" +
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
                "create_time string,\n" +
                "source_id string,\n" +
                "source_type string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "ts string,\n" +
                "row_time as TO_TIMESTAMP(FROM_UNIXTIME(cast(ts as bigint))),\n" +
                "watermark for row_time as row_time" +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_detail", "dwd_trade_pay_detail_suc"));

        // TODO 4. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` String,\n" +
                "`table` String,\n" +
                "`type` String,\n" +
                "`data` map<String, String>,\n" +
                "`old` map<String, String>,\n" +
                "`proc_time` as PROCTIME(),\n" +
                "`ts` string,\n" +
                "row_time as TO_TIMESTAMP(FROM_UNIXTIME(cast(ts as bigint))),\n" +
                "watermark for row_time as row_time" +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_trade_pay_detail_suc"));


        // TODO 5. 筛选支付成功数据
        Table paymentInfo = tableEnv.sqlQuery("select\n" +
                        "data['user_id'] user_id,\n" +
                        "data['order_id'] order_id,\n" +
                        "data['payment_type'] payment_type,\n" +
                        "data['callback_time'] callback_time,\n" +
                        "row_time,\n" +
                        "`proc_time`,\n" +
                        "ts\n" +
                        "from topic_db\n" +
                        "where `table` = 'payment_info'\n" +
                "and `type` = 'update'\n" +
                "and data['payment_status']='1602'"
        );
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        // TODO 6. 建立 HBase-LookUp 字典表
//        tableEnv.executeSql(PhoenixUtil.getBaseDicDDL());
        tableEnv.executeSql(MySqlUtil.getBaseDicLookUpDDL());
        // TODO 7. 关联 3 张表获得支付成功宽表
        Table joinTable = tableEnv.sqlQuery("" +
                "select\n" +
                "pi.order_id order_id,\n" +
                "pi.payment_type payment_type_code,\n" +
                "dic_name payment_type_name,\n" +
                "pi.callback_time,\n" +
                "pi.row_time,\n" +
                "pi.ts\n" +
                "from payment_info pi\n" +
                "join `base_dic` for system_time as of pi.proc_time as dic\n" +
                "on pi.payment_type = dic.dic_code");
        tableEnv.createTemporaryView("join_table", joinTable);

        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "od.id order_detail_id,\n" +
                "od.order_id,\n" +
                "od.user_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "od.province_id,\n" +
                "od.activity_id,\n" +
                "od.activity_rule_id,\n" +
                "od.coupon_id,\n" +
                "pi.payment_type_code,\n" +
                "pi.payment_type_name,\n" +
                "pi.callback_time,\n" +
                "od.source_id,\n" +
                "od.source_type source_type_code,\n" +
                "od.source_type_name,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount split_payment_amount,\n" +
                "pi.ts \n" +
                "from dwd_trade_order_detail od, join_table pi\n" +
                "where od.order_id = pi.order_id\n " +
                "and od.row_time >= pi.row_time - INTERVAL '15' MINUTE \n" +
                "and od.row_time <= pi.row_time + INTERVAL '5' SECOND");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 8. 创建 Kafka dwd_trade_pay_detail_suc 表
        tableEnv.executeSql("create table dwd_trade_pay_detail_suc(\n" +
                "order_detail_id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "payment_type_code string,\n" +
                "payment_type_name string,\n" +
                "callback_time string,\n" +
                "source_id string,\n" +
                "source_type_code string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_payment_amount string,\n" +
                "ts string,\n" +
                "primary key(order_detail_id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_detail_suc"));

        // TODO 9. 将关联结果写入 Upsert-Kafka 表
//        resultTable.insertInto("dwd_trade_pay_detail_suc");
        tableEnv.executeSql("insert into dwd_trade_pay_detail_suc select * from result_table");
    }
}
