package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/**
 * ClassName: TableProcessFunction
 * Package: com.atguigu.gmall.realtime.common.function
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/21 10:31
 * @Version 1.0
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;

    private Map<String, TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    //    open方法
    @Override
    public void open(Configuration parameters) throws Exception {
        //        将配置表中的配置信息预加载到程序configMap中
        Connection mySQLConnection = JdbcUtil.getMySQLConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mySQLConnection, "select * from gmall_config.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            String sourceTable = tableProcessDim.getSourceTable();
            configMap.put(sourceTable, tableProcessDim);
        }
        JdbcUtil.closeMySQLConnection(mySQLConnection);
    }
//      处理主流数据

    //    {"database":"gmall-flink","table":"user_info","type":"update","ts":1752743436,"xid":120579,"commit":true,"data":{"id":5,"login_name":"2c7eysrvb986","nick_name":"康星","passwd":null,"name":"顾淑","phone_num":"13199928898","email":"8clwiw7w9eqn@163.com","head_img":null,"user_level":"1","birthday":"1996-03-08","gender":null,"create_time":"2025-06-08 00:00:00","operate_time":"2025-06-10 00:00:00","status":null},"old":{"phone_num":"13944538948","operate_time":"2025-06-09 00:00:00"}}
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        String table = jsonObject.getString("table");
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcessDim tableProcessDim = null;
        if ((tableProcessDim = broadcastState.get(table)) != null
                || (tableProcessDim = configMap.get(table)) != null) {
            //如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据

//    "data":{"id":5,"login_name":"2c7eysrvb986","nick_name":"康星","passwd":null,"name":"顾淑","phone_num":"13199928898","email":"8clwiw7w9eqn@163.com","head_img":null,"user_level":"1","birthday":"1996-03-08","gender":null,"create_time":"2025-06-08 00:00:00","operate_time":"2025-06-10 00:00:00","status":null}
            // 将维度数据继续向下游传递(只需要传递data属性内容即可)
            JSONObject data = jsonObject.getJSONObject("data");
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(data, sinkColumns);

            //在向下游传递数据前，补充对维度数据的操作类型属性
            String type = jsonObject.getString("type");
            data.put("type", type);
            out.collect(Tuple2.of(data, tableProcessDim));

        }


    }

    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        String op = tableProcessDim.getOp();
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//        获取维度表名称
        String sourceTable = tableProcessDim.getSourceTable();
        if ("d".equals(op)) {
//            从配置表中删除了一条维度数据，将对应的配置信息也从广播状态中删除
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
//            对配置表进行了读取，更新，将最新的配置信息放到了广播状态中
            broadcastState.put(sourceTable, tableProcessDim);
            configMap.put(sourceTable, tableProcessDim);
        }
    }

    //过滤掉不需要传递的字段
    //dataJsonObj  {"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"555","id":1}
    //sinkColumns  id,tm_name
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));

    }


}