package com.atguigu.gmall.realtime.common.util;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;


/**
 * ClassName: KafkaUtil
 * Package: com.atguigu.gmall.realtime.common.util
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/20 17:05
 * @Version 1.0
 */
public class KafkaUtil {
    //获取KafkaSource
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                //.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
                //在生产环境中，一般为了保证消费的精准一次性，需要手动维护偏移量，KafkaSource->KafkaSourceReader->存储偏移量变量
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                // 从最末尾位点开始消费
                .setStartingOffsets(OffsetsInitializer.latest())
                //注意：如果使用Flink提供的SimpleStringSchema对String类型的消息进行反序列化，如果消息为空，会报错
                //.setValueOnlyDeserializer(new SimpleStringSchema())
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

    public static KafkaSink getKafkaSink(String topicName) {
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                //当前配置决定是否开启事务，保证写到kafka数据的精准一次
/*                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                //设置事务Id的前缀
                .setTransactionalIdPrefix("dwd_base_log_")
                //设置事务的超时时间   检查点超时时间 <     事务的超时时间 <=事务最大超时时间
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")*/
                .build();

        return sink;

    }

    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> sink = KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> element, KafkaSinkContext context, Long timestamp) {
                                String sinkTable = element.f1.getSinkTable();
                                System.out.println("sinkTable:"+sinkTable);
                                return new ProducerRecord<byte[], byte[]>(sinkTable, element.f0.toJSONString().getBytes());


                            }
                        }
                )
                //当前配置决定是否开启事务，保证写到kafka数据的精准一次
/*                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                //设置事务Id的前缀
                .setTransactionalIdPrefix("dwd_base_log_")
                //设置事务的超时时间   检查点超时时间 <     事务的超时时间 <=事务最大超时时间
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")*/
                .build();

        return sink;

    }
}
