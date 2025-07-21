package com.atguigu.gmall;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: FlinkCdcTest
 * Package: com.atguigu.gmall
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/21 9:33
 * @Version 1.0
 */
public class FlinkCdcTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop202")
                .port(3306)
                .databaseList("gmall_config") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("gmall_config"+"."+"table_process_dim") // set captured table
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<String> source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Mysql Source");
        source.print();
        env.execute();
    }
}
