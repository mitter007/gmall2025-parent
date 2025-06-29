package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.utils.MyKafkaUtil;
import com.atguigu.gmall.realtime.app.utils.MySqlUtil;
import com.atguigu.gmall.realtime.app.utils.PhoenixUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *
 */
public class Dwd02_DwdInteractionCommentInfo {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.检查点相关设置(略)

        // TODO 3. 从Kafka 读取业务数据，封装为Flink SQL表
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_interaction_comment_info"));

        // TODO 4. 从业务表中过滤出评论表数据

//        proc_time 是从哪里来的
        Table commentInfo = tableEnv.sqlQuery(
            "select " +
                "data['id'] id," +
                "data['user_id'] user_id," +
                "data['sku_id'] sku_id," +
                "data['appraise'] appraise," +
                "data['comment_txt'] comment_txt," +
                "ts," +
                "proc_time" +
                " from `topic_db` " +
                " where `table` = 'comment_info'" +
                " and `type` = 'insert' ");
        tableEnv.createTemporaryView("comment_info", commentInfo);

        // TODO 5. 建立 MySQL-LookUp 字典表
//        在这里维度表数据明明写到hbase中了为什么还又从mysql中拿呢。
        tableEnv.executeSql(MySqlUtil.getBaseDicLookUpDDL());
//        从Phoenix中获取
//        tableEnv.executeSql(PhoenixUtil.getBaseDicDDL());
      tableEnv.sqlQuery("" +
                "select * from base_dic ");
//        System.out.println(table.toString());
        // TODO 6. 关联两张表获得评论明细表
        Table resultTable = tableEnv.sqlQuery("select " +
            "ci.id," +
            "user_id," +
            "sku_id," +
            "appraise," +
            "dic_name appraise_name," +
            "comment_txt," +
            "ts" +
            " from comment_info ci " +
            " join base_dic for system_time as of ci.proc_time as dic " +
            " on ci.appraise=dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        //tableEnv.executeSql("select * from result_table").print();

        // TODO 7. 建立 Upsert-Kafka dwd_interaction_comment 表
        tableEnv.executeSql("" +
            "create table dwd_interaction_comment(" +
            "id string," +
            "user_id string," +
            "sku_id string," +
            "appraise string," +
            "appraise_name string," +
            "comment_txt string," +
            "ts string," +
            "primary key(id) not enforced" +
            ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_interaction_comment"));

        // TODO 8. 将关联结果写入 Upsert-Kafka 表

//        resultTable.insertInto("dwd_interaction_comment");
        tableEnv.executeSql("" +
            "insert into dwd_interaction_comment select * from result_table");
    }

}
