package com.atguigu.gmall.realtime.app.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;

public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "hadoop202:9092,hadoop203:9092,hadoop204:9092";

    //获取kafkaSource
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setTopics(topic)
                .setGroupId(groupId)
                //从flink状态中维护的偏移量位置开发消费，如果状态中还没有维护偏移量，从kafka的最新位置开发消费
                //如果要做如下配置：需要将检查点打开,生产环境这么设置
                // .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                //为了保证一致性，在消费数据的时候，我们这里只读取已提交的消息
                // .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
                // 在学习的时候，我们可以直接从kafka最新位置开始读取
                .setStartingOffsets(OffsetsInitializer.latest())

                //注意：如果使用SimpleStringSchema进行反序列化的话，不能处理空消息
                //      throw new NullPointerException();
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if (message != null) {
                                    return new String(message);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();
        return kafkaSource;
    }

    //获取kakfaSink
    public static KafkaSink<String> getKafkaSink(String topic) {
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                //注意：要想保证写到kafka的精准一次，需要做如下几个操作
                //1.DeliveryGuarantee.EXACTLY_ONCE   底层会开启事务
                //2.kafka的消费者的隔离级别  需要设置为读已提交
                //3.需要开启检查点
                //4.设置事务超时时间，应该大于检查点超时时间，并小于等于最大时间
                //5.指定事务ID前缀
//                 .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                 .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
//                 .setTransactionalIdPrefix("事务ID前缀")
                .build();
        return kafkaSink;
    }

    //获取从topic_db主题中读取数据的建表语句
    public static String getTopicDbDDL(String groupId) {
        return "CREATE TABLE topic_db (\n" +
                "  `database` string,\n" +
                "  `table` string,\n" +
                "  `type` string,\n" +
                "  `ts` string,\n" +
                "  `data` MAP<string, string>,\n" +
                "  `old` MAP<string, string>,\n" +
                "  `proc_time` as proctime()\n" +
                ") " + getKafkaDDL("topic_db", groupId);
    }

    //获取kafka连接属性
//注意： 'scan.startup.mode' = 'group-offsets',和 'properties.auto.offset.reset' = 'latest'结合
//表示先从消费者组提交的偏移量消费，如果不存在从最新的位置开始消费
    public static String getKafkaDDL(String topic, String groupId) {
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                //在生产环境下，为了保证消费的一致性，需要做如下的配置
//"  'scan.startup.mode' = 'group-offsets',\n" +
                //"  'properties.auto.offset.reset' = 'latest',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    //获取upsert-kafka的连接属性
    public static String getUpsertKafkaDDL(String topic) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

}
