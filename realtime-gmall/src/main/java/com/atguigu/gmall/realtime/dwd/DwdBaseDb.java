package com.atguigu.gmall.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BaseDbTableProcessFunction;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * ClassName: DwdBaseDb
 * Package: com.atguigu.gmall.realtime.dwd
 * Description: 事实表动态分流 用flinkcdc 读取配置表的
 *
 * @Author JWT
 * @Create 2025/7/21 16:44
 * @Version 1.0
 */
public class DwdBaseDb extends BaseApp {

    private static final Logger logger = LogManager.getLogger(DwdBaseDb.class);

    public static void main(String[] args) throws Exception {
        new DwdBaseDb().start(
                1003,
                4, Constant.TOPIC_DB
                ,
                "dwd_base_db"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");
                    if (!type.startsWith("bootstrap-")) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    logger.error("数据转换失败：" + value);
                    throw new RuntimeException(e);
                }
            }
        });
//      jsonDS.print("json>>>>");
        SingleOutputStreamOperator<TableProcessDwd> readTableProcessDwdDS = readTableProcessDwd(env);
//        readTableProcessDwdDS.print("dwd>>");
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = readTableProcessDwdDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonDS.connect(broadcastDS);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> tup2DS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));
        sinkToKafka(tup2DS);


    }

    public SingleOutputStreamOperator<TableProcessDwd> readTableProcessDwd(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlcdc("gmall_config", "table_process_dwd");

        DataStreamSource<String> source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");


// {"before":null,"after":{"source_table":"activity_sku","sink_table":"dim_activity_sku","sink_family":"info","sink_columns":"id,activity_id,sku_id,create_time","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1753061819189,"transaction":null}
        SingleOutputStreamOperator<TableProcessDwd> map = source.map(new MapFunction<String, TableProcessDwd>() {
            @Override
            public TableProcessDwd map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String op = jsonObject.getString("op");
                TableProcessDwd tableProcessDwd = null;
                if ("d".equals(op)) {
                    tableProcessDwd = jsonObject.getObject("before", TableProcessDwd.class);
                } else {
                    tableProcessDwd = jsonObject.getObject("after", TableProcessDwd.class);
                }
                tableProcessDwd.setOp(op);

                return tableProcessDwd;
            }
        });
        return map;

    }

    private static void sinkToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> tup2DS) {
        tup2DS.sinkTo(FlinkSinkUtil.getKafkaSink());
    }


}
