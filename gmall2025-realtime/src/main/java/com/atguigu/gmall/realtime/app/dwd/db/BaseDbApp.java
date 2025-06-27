package com.atguigu.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.bean.TableProcess;
import com.atguigu.gmall.realtime.app.func.BaseDbTableProcessFunction;
import com.atguigu.gmall.realtime.app.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

// * dwd简单事实表动态分流处理
// */
public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
       
        //TODO 2.检查点相关的设置(略)
        
        //TODO 3.从kafka的topic_db主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String topic = "topic_db";
        String groupId = "base_db_app_group";
        //3.2 创建消费者对象
        KafkaSource<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //3.3 消费数据 将数据封装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        //TODO 4.简单的ETL以及对读取的数据进行类型的转换  jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> filterDS = kafkaStrDS.process(
            new ProcessFunction<String, JSONObject>() {
                @Override
                public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                    try {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
//                        为什么下面要这样写
                        if (!jsonObj.getString("type").equals("bootstrap-start")
                            && !jsonObj.getString("type").equals("bootstrap-complete")
                            && !jsonObj.getString("type").equals("bootstrap-insert")) {
                            out.collect(jsonObj);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        );
        //{"database":"gmall2023","xid":42787,"data":{"create_time":"2023-01-29 15:43:48","user_id":493,"appraise":"1204","comment_txt":"评论内容：94631686192165391816122322992153392279151463121572","sku_id":11,"id":1622138907505061891,"spu_id":3,"order_id":221},"xoffset":3353,"type":"insert","table":"comment_info","ts":1675583028}
        // filterDS.print(">>>");
        //TODO 5.使用FlinkCDC读取配置表数据
        Properties props = new Properties();
        props.setProperty("useSSL","false");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("hadoop202")
            .port(3306)
            .databaseList("gmall_config") // set captured database
            .tableList("gmall_config.table_process") // set captured table
            .username("root")
            .password("000000")
            .startupOptions(StartupOptions.initial())
            .jdbcProperties(props)
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();
        DataStreamSource<String> mySqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        //TODO 6.将读取到的配置信息进行广播
        MapStateDescriptor<String, TableProcess> mapStateDescriptor
            = new MapStateDescriptor<String, TableProcess>("mapStateDescriptor",String.class,TableProcess.class);
        BroadcastStream<String> broadcastDS = mySqlDS.broadcast(mapStateDescriptor);

        //TODO 7.将主流业务数据和广播流中的配置数据进行关联
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastDS);

        //TODO 8.对关联之后的数据进行处理
        SingleOutputStreamOperator<JSONObject> realDS = connectDS.process(
            new BaseDbTableProcessFunction(mapStateDescriptor)
        );
        //TODO 9.将处理后的业务数据发送到kafka的不同的主题中
        realDS.print(">>>>");
        realDS.sinkTo(
            MyKafkaUtil.getKafkaSinkBySchema(
                new KafkaRecordSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, KafkaSinkContext context, Long timestamp) {
                        String topic = jsonObj.getString("sink_table");
                        jsonObj.remove("sink_table");
                        return new ProducerRecord<byte[], byte[]>(topic,jsonObj.toJSONString().getBytes());
                    }
                }
            )
        );
        env.execute();
    }
}
