package com.atguigu.gmall.realtime.common.util;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

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
    public static KafkaSource<String> getKafkaSource(String bootstrapServers, String topic, String groupId) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return null;
                    }

                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message);
                        } else return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }
                })
                .build();

        return source;

    }
}
