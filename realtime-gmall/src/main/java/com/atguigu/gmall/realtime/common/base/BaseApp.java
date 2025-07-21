package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: BaseApp
 * Package: com.atguigu.gmall.realtime
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/20 15:59
 * @Version 1.0
 */
public abstract class BaseApp {
    public void start(Integer port, Integer parallelism, String topic, String groupId) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        DataStreamSource<String> dataStreamSource = env.fromSource(KafkaUtil.getKafkaSource(Constant.KAFKA_BROKERS, topic, groupId), WatermarkStrategy.noWatermarks(), "kafka source");
        handle(env, dataStreamSource);
        env.execute();

    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> dataStreamSource);
}
