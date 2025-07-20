package com.test;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * ClassName: Test01_Doris01
 * Package: com.test
 * Description:
 *
 * @Author JWT
 * @Create 2025/6/29 11:02
 * @Version 1.0
 */
public class Test01_Doris01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.enableCheckpointing(30000);
        DorisSink.Builder<String> builder = DorisSink.builder();


//        构造者设计模式
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("hadoop202:7030")
                .setTableIdentifier("test.table1")
                .setUsername("root")
                .setPassword("aaaaaa")
                .build();

        Properties properties = new Properties();
// 上游是json数据的时候，需要开启以下配置
        properties.setProperty("read_json_by_line", "true");
        properties.setProperty("format", "json");

// 上游是 csv 写入时，需要开启配置
//properties.setProperty("format", "csv");
//properties.setProperty("column_separator", ",");

        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setLabelPrefix("label-doris")
                .setDeletable(false)
                //.setBatchMode(true)  开启攒批写入
                .setStreamLoadProp(properties)
                .build();

        DorisSink<String> build = builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisOptions).build();


        DataStreamSource<String> source = env
                .fromElements(
                        "{\"siteid\": \"601\", \"citycode\": \"1001\", \"username\": \"ww\",\"pv\": \"100\"}");


        source.sinkTo(builder.build());
        env.execute("doris test");
    }
}
