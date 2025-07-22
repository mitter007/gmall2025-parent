package com.atguigu.gmall.realtime.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.HBaseSinkFunction;
import com.atguigu.gmall.realtime.common.function.TableProcessFunction;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HbaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * ClassName: DimApp
 * Package: com.atguigu.gmall.realtime.dim
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/20 21:29
 * @Version 1.0
 */
public class DimApp extends BaseApp {
    private static final Logger logger = LogManager.getLogger(DimApp.class);

    public static void main(String[] args) throws Exception {
        new DimApp().start(
                1001,
                1, Constant.TOPIC_DB
                ,
                "dim_app"
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> dataStreamSource) {
//        主流信息
        SingleOutputStreamOperator<JSONObject> jsonDS = etl(dataStreamSource);

        //TODO 使用FlinkCDC读取配置表中的配置信息  配置流
        SingleOutputStreamOperator<TableProcessDim> tPDS = readTableProcess(env);

        //TODO 根据配置表中的配置信息到HBase中执行建表或者删除表操作
        tPDS   = createHbaseTable(tPDS);
        //TODO 过滤维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectDS = connect(tPDS, jsonDS);
        writeToHBase(connectDS);
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> dataStreamSource) {
        SingleOutputStreamOperator<JSONObject> process = dataStreamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

//  {"database":"gmall-flink","table":"user_info","type":"update","ts":1752743436,"xid":120612,"commit":true,"data":{"id":56,"login_name":"status":null},"old":{"operate_time":null}}
                JSONObject jsonObject = JSON.parseObject(s);
                String database = jsonObject.getString("database");
                String type = jsonObject.getString("type");
                String data = jsonObject.getString("data");
                if (database.equals("gmall-flink") && (
                        type.equals("insert")
                                || type.equals("update")
                                || type.equals("delete")
                                || type.equals("bootstrap-insert")
                ) && data != null
                        && data.length() > 2
                ) {
                    collector.collect(jsonObject);
                }


            }
        });

        return process;
    }


    public SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlcdc("gmall_config", "table_process_dim");

        DataStreamSource<String> source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql source");


// {"before":null,"after":{"source_table":"activity_sku","sink_table":"dim_activity_sku","sink_family":"info","sink_columns":"id,activity_id,sku_id,create_time","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1753061819189,"transaction":null}
        SingleOutputStreamOperator<TableProcessDim> map = source.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String op = jsonObject.getString("op");
                TableProcessDim tableProcessDim = null;
                if ("d".equals(op)) {
                    tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                } else {
                    tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                }
                tableProcessDim.setOp(op);

                return tableProcessDim;
            }
        });
        return map;

    }

    private static SingleOutputStreamOperator<TableProcessDim> createHbaseTable(SingleOutputStreamOperator<TableProcessDim> tPDS) {
        tPDS = tPDS.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {
                            private Connection hbaseConn;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                hbaseConn = HbaseUtil.getHbaseconnection();
                            }

                            @Override
                            public TableProcessDim map(TableProcessDim tp) {
                                String op = tp.getOp();
                                String sourceTable = tp.getSourceTable();
                                String sinkTable = tp.getSinkTable();
                                //获取在HBase中建表的列族
                                String[] sinkFamilies = tp.getSinkFamily().split(",");
                                if ("d".equals(op)) {
                                    HbaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                                } else if ("r".equals(op)) {
                                    //从配置表中读取了一条数据或者向配置表中添加了一条配置   在hbase中执行建表
                                    HbaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);

                                } else {
                                    //对配置表中的配置信息进行了修改   先从hbase中将对应的表删除掉，再创建新表
                                    HbaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                                    HbaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                                }
                                return tp;


                            }

                            @Override
                            public void close() throws Exception {
                                hbaseConn.close();
                            }
                        }
        ).setParallelism(1);
        return tPDS;


    }

    public static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<TableProcessDim> tpDS, SingleOutputStreamOperator<JSONObject> jsonObjDS) {
//
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor",
//                为啥这里是String.class
                String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcast = tpDS.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcast);
//        处理关联后的数据是否为维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> process = connectDS.process(new TableProcessFunction(mapStateDescriptor));

        return process;


    }

    private static void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.addSink(new HBaseSinkFunction());
    }

}
