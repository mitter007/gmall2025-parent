package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.bean.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

public class BaseDbTableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    private Map<String,TableProcess> configMap = new HashMap<>();

    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop202:3306/gmall_config?" +
            "user=root&password=000000&useUnicode=true&" +
            "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");

        String sql = "select * from gmall_config.table_process where sink_type = 'dwd'";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        ResultSet rs = preparedStatement.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();

        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                String columnValue = rs.getString(i);
                jsonObj.put(columnName,columnValue);
            }
            TableProcess tableProcess = JSON.toJavaObject(jsonObj,TableProcess.class);
            if(tableProcess != null){
                String sourceTable = tableProcess.getSourceTable();
                String sourceType = tableProcess.getSourceType();
                String key = sourceTable + "_" + sourceType;
                configMap.put(key, tableProcess);
            }
        }

        rs.close();
        preparedStatement.close();
        conn.close();

    }

    //处理主流数据
    //{"database":"gmall2023","xid":42787,"data":{"create_time":"2023-01-29 15:43:48","user_id":493,"appraise":"1204","comment_txt":"评论内容：94631686192165391816122322992153392279151463121572","sku_id":11,"id":1622138907505061891,"spu_id":3,"order_id":221},"xoffset":3353,"type":"insert","table":"comment_info","ts":1675583028}
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        String tableName = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        String key = tableName + "_" + type;
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = null;
        if((tableProcess = broadcastState.get(key))!=null ||
            (tableProcess = configMap.get(key))!=null ){
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");

            //过滤掉不需要向下游传递的属性
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(dataJsonObj,sinkColumns);
            //在向下游传递数据前，补充输出目的地
            String sinkTable = tableProcess.getSinkTable();
            dataJsonObj.put("sink_table",sinkTable);
dataJsonObj.put("ts",jsonObj.getLong("ts"));

            //将事实数据向下游传递
            out.collect(dataJsonObj);
        }
    }

    private void filterColumn(JSONObject dataJsonObj, String sinkColumns) {
        String[] columnArr = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columnArr);
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry->!columnList.contains(entry.getKey()));
    }

    //处理广播流数据
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
        //为了处理方便  将json字符串转换为json对象
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        String op = jsonObj.getString("op");
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        if ("d".equals(op)) {
            //如果对配置表做了删除操作，需要将配置信息从广播状态中删除掉
            TableProcess before = jsonObj.getObject("before", TableProcess.class);
            String sinkType = before.getSinkType();
            if ("dwd".equals(sinkType)) {
                String sourceTable = before.getSourceTable();
                String sourceType = before.getSourceType();
                String key = sourceTable + "_" + sourceType;
                broadcastState.remove(key);
                configMap.remove(key);
            }
        } else {
            //如果对配置表做了删除外的其它操作，需要将配置信息放到广播状态中
            TableProcess after = jsonObj.getObject("after", TableProcess.class);
            String sinkType = after.getSinkType();
            if ("dwd".equals(sinkType)) {
                String sourceTable = after.getSourceTable();
                String sourceType = after.getSourceType();
                String key = sourceTable + "_" + sourceType;
                broadcastState.put(key, after);
                configMap.put(key,after);
            }
        }
    }
}
