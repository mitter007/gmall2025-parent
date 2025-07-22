package com.atguigu.gmall.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcessDim;
import com.atguigu.gmall.realtime.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.dim.DimApp;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * ClassName: DwdBaseDb
 * Package: com.atguigu.gmall.realtime.dwd
 * Description:
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
        SingleOutputStreamOperator<JSONObject> process = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
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
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlcdc("gmall_config", "table_process_dwd");
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        mysqlDS.process(new ProcessFunction<String, TableProcessDwd>() {
            @Override
            public void processElement(String value, ProcessFunction<String, TableProcessDwd>.Context ctx, Collector<TableProcessDwd> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
//                获取操作类型
                String op = jsonObject.getString("op");
                TableProcessDwd tp = null;
                if ("d".equals(op)) {
                    tp = jsonObject.getObject("before", TableProcessDwd.class);

                } else {
                    tp = jsonObject.getObject("after", TableProcessDwd.class);
                }
                tp.setOp(op);
                out.collect(tp);

            }
        });


    }
}
