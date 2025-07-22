package com.atguigu;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * ClassName: ${NAME}
 * Package: com.atguigu
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/22 10:29
 * @Version 1.0
 */
public class KafkaMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        Properties properties = new Properties();
        properties.load(KafkaMain.class.getClassLoader().getResourceAsStream("consumer.properties"));

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        boolean flag = true;
        int i = 0;

        while (flag) {
//            long timestampSeconds = Instant.now().getEpochSecond();
            long timestampSeconds = System.currentTimeMillis();
            try {
                producer.send(new ProducerRecord<>("do3", "{\"uid\":" + i++ + ",\"event_id\":\"aaa\",\"properties\":{\"url\":\"a\",\"ref\":\"x\"},\"action_time\":" + timestampSeconds + "}"));
            } catch (Exception e) {
                System.out.println("发生错误了......");
                throw new RuntimeException(e);
            }

            Thread.sleep(3000);



        }
    }
}