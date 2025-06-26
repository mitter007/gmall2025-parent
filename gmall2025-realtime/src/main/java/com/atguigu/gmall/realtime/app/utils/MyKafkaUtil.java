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
            .setStartingOffsets(OffsetsInitializer.earliest())

            //注意：如果使用SimpleStringSchema进行反序列化的话，不能处理空消息
             .setValueOnlyDeserializer(new SimpleStringSchema()) //      throw new NullPointerException();
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

}
