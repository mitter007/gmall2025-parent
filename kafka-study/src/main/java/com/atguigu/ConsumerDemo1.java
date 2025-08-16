package com.atguigu;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;

public class ConsumerDemo1 {
    public static void main(String[] args) throws IOException {

        Properties props = new Properties();
        // 加载properties配置文件，并自动解析
        props.load(ConsumerDemo1.class.getClassLoader().getResourceAsStream("consumer.properties"));

        // 构造消费者客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅主题 topic;  各主题的各分区如何在组中分配，是自动均衡分配的
        //consumer.subscribe(Collections.singletonList("stu"));


        //  手动分配主题分区给消费者
        TopicPartition tp0 = new TopicPartition("stu", 0);
        TopicPartition tp1 = new TopicPartition("stu", 1);
//        TopicPartition tp2 = new TopicPartition("stu", 2);
        //  手动分配主题分区给消费者
        consumer.assign(Arrays.asList(tp0,tp1));
        // 手动指定各分区的消费起始位移
//        consumer.seek(tp0,255);
//        consumer.seek(tp1,240);
//        consumer.seek(tp2,245);


        // 拉取数据
        while (true) {

            // 这里所拉取的数据，可能包含多个主题、多个分区的数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            // 从拉取到的数据中，获取指定topic指定partition的数据
            //TopicPartition tp0 = new TopicPartition("stu", 0);
            //List<ConsumerRecord<String, String>> tp0Records = records.records(tp0);

            // 从拉取到的数据中，获取指定topic的数据
            //Iterable<ConsumerRecord<String, String>> rc = records.records("stu");

            // 把本次拉取的所有数据转成一个迭代器
            Iterator<ConsumerRecord<String, String>> iter = records.iterator();

            while(iter.hasNext()){
                // 迭代到一条数据
                ConsumerRecord<String, String> record = iter.next();

                // 获取数据的header
                Headers headers = record.headers();
                Iterator<Header> headerIter = headers.iterator();
                while(headerIter.hasNext()){
                    Header header = headerIter.next();
                    String key = header.key();
                    byte[] valueBytes = header.value();
                    System.out.println("本条数据的header: " + key + "->" + new String(valueBytes));
                }


                // 获取数据的key和value
                String recordKey = record.key();
                String recordValue = record.value();
                System.out.println(String.format("本条数据的key: %s , value: %s",recordKey,recordValue));


                // 获取数据所属的topic
                String topic = record.topic();
                System.out.println(String.format("本条数据所属的topic: %s",topic));


                // 获取数据所属的partition
                int partition = record.partition();
                System.out.println(String.format("本条数据所属的partition: %d",partition));

                // 获取本条数据的offset
                long offset = record.offset();
                System.out.println(String.format("本条数据的offset: %d",offset));

                // 获取本条数据所属的leader纪元
                Optional<Integer> epoch = record.leaderEpoch();
                System.out.println("当前的leader epoch: " + epoch);

                // 获取本条数据的时间戳
                // 默认是 CREATE_TIME
                // kafka中的数据的时间戳，有3种类型： NO_TIMESTAMP_TYPE,CREATE_TIME,LOG_APPEND_TIME
                long timestamp = record.timestamp();
                System.out.println(String.format("本条数据的timestamp: %d",timestamp));
                TimestampType timestampType = record.timestampType();
                System.out.println("本条数据的timestamp类型为：" + timestampType);




                System.out.println("--------------------------------------------");


            }





        }


    }
}
