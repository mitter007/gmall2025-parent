package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_02_SQL {
    public static void main(String[] args) throws Exception {
        // TODO 1. 准备环境
        // 1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.2 表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 创建动态表
        tableEnv.executeSql("CREATE TABLE user_info (\n" +
                "id INT,\n" +
                "name STRING,\n" +
                "age INT,\n" +
                "primary key(id) not enforced\n" +
                ") WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = 'hadoop202'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '000000'," +
                "'database-name' = 'gmall_config'," +
                "'table-name' = 'table_process'" +
                ")");

        tableEnv.executeSql("select * from user_info").print();

        // TODO 3. 执行任务
        env.execute();
    }
}
