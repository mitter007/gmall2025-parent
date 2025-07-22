package com.atguigu.gmall.realtime.dwd;

import com.atguigu.gmall.realtime.common.base.BaseSQL;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwdInteractionCommentInfo
 * Package: com.atguigu.gmall.realtime.dwd
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/22 11:12
 * @Version 1.0
 */
public class DwdCartAddInfo extends BaseSQL {
    public static void main(String[] args) {
        new DwdCartAddInfo().start(
                1005,
                4,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 从kafka的topic_db主题中读取数据 创建动态表       ---kafka连接器
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);
        //TODO 过滤出架构数据                                ---where table='comment_info'  type='insert'

        String s ="select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['cart_price'] cart_price,\n" +
                "   if(type='insert',`data`['sku_num'], CAST((CAST(data['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) AS STRING)) sku_num,\n" +
                "    `data`['img_url'] img_url,\n" +
                "    `data`['sku_name'] sku_name,\n" +
                "    `data`['is_checked'] is_checked,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['operate_time'] operate_time,\n" +
                "    `data`['is_ordered'] is_ordered,\n" +
                "    `data`['order_time'] order_time,\n" +
                "    `data`['source_type'] source_type,\n" +
                "    ts,\n" +
                "    pt\n" +
                "from topic_db where `table`='cart_info' and" +
                "    type = 'insert'\n" +
                "    or\n" +
                "    (type='update' and `old`['sku_num'] is not null and (CAST(data['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT)))";
//        System.out.println(s);
//        加购这个动作其实关乎数量的增加与减少

//        "   if(type='insert',`data`['sku_num'], CAST((CAST(data['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) AS STRING)) sku_num,\n" +
        Table cartInfo = tableEnv.sqlQuery(s);


//        cartInfo.execute().print();
        tableEnv.createTemporaryView("cart_info", cartInfo);
        //TODO 将过滤出来的加购数据写到kafka主题中
        //创建动态表和要写入的主题进行映射

        //        {"id":"2388","user_id":"318","sku_id":"1","sku_num":"1","ts":1752031974}
        tableEnv.executeSql(" create table " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )" + FlinkSQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
//        tableEnv.executeSql(" select id,user_id,sku_id,sku_num,ts from cart_info").print();
        // 写入
        tableEnv.executeSql("insert into " + Constant.TOPIC_DWD_TRADE_CART_ADD + " select id,user_id,sku_id,sku_num,ts from cart_info");

    }

}
