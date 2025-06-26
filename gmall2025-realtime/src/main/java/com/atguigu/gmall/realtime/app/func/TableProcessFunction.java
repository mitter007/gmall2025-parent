package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.bean.TableProcess;
import com.atguigu.gmall.realtime.app.common.GmallConfig;
import com.atguigu.gmall.realtime.app.utils.DruidDSUtil;
import com.atguigu.gmall.realtime.app.utils.PhoenixUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * dim层处理主流数据以及广播流数据
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    private Map<String,TableProcess> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置表中的配置信息加载到内存中
        //注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        //建立连接
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop202:3306/gmall_config?" +
            "user=root&password=000000&useUnicode=true&characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");
        //获取数据库操作对象
        String sql = "select * from gmall_config.table_process where sink_type='dim'";
        PreparedStatement ps = conn.prepareStatement(sql);
        //执行SQL语句
        ResultSet rs = ps.executeQuery();
        //处理结果集
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()){
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                jsonObj.put(columnName,columnValue);
            }
            TableProcess tableProcess = JSON.toJavaObject(jsonObj, TableProcess.class);
            configMap.put(tableProcess.getSourceTable(),tableProcess);
        }
        //释放资源
        rs.close();
        ps.close();
        conn.close();
    }

    //处理主流数据
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //获取当前处理的业务表的表名
        String tableName = jsonObj.getString("table");
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //根据业务表名到广播状态中获取对应的配置信息
        TableProcess tableProcess = null;

        //判断是否是维度数据
        if((tableProcess = broadcastState.get(tableName))!=null
            ||(tableProcess = configMap.get(tableName))!=null){
            //说明处理的数据是维度数据
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            //过滤掉不需要向下游传递的属性
            // {"tm_name":"CLS","logo_url":"fdafa","id":12}
            String sinkColumns = tableProcess.getSinkColumns();
            //{"tm_name":"FDA","id":12}
            filterColumn(dataJsonObj, sinkColumns);
            //获取目的地表名
            String sinkTable = tableProcess.getSinkTable();
            //在向下游传递数据前，补充输出目的地属性  {"tm_name":"CLS","sink_table":"dim_base_trademark","id":12}
            dataJsonObj.put("sink_table",sinkTable);
            //补充操作类型
            dataJsonObj.put("type",jsonObj.getString("type"));
            //将维度数据向下游传递
            out.collect(dataJsonObj);
        }
    }

    //过滤掉不需要向下游传递的属性
    private void filterColumn(JSONObject dataJsonObj, String sinkColumns) {
        String[] columnArr = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columnArr);

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry->!columnList.contains(entry.getKey()));

    }

    //处理广播流数据
    //"op":"r": {"before":null,"after":{"source_table":"financial_sku_cost","source_type":"ALL","sink_table":"dim_financial_sku_cost","sink_type":"dim","sink_columns":"id,sku_id,sku_name,busi_date,is_lastest,sku_cost,create_time","sink_pk":"id","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1675066119553,"snapshot":"false","db":"gmall0815_config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1675066119553,"transaction":null}
    //"op":"c": {"before":null,"after":{"source_table":"a","source_type":"ALL","sink_table":"dim_a","sink_type":"dim","sink_columns":"id,name","sink_pk":"id","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1675066193000,"snapshot":"false","db":"gmall0815_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000011","pos":1086544,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1675066191816,"transaction":null}
    //"op":"u": {"before":{"source_table":"a","source_type":"ALL","sink_table":"dim_ab","sink_type":"dim","sink_columns":"id,name","sink_pk":"id","sink_extend":null},"after":{"source_table":"a","source_type":"ALL","sink_table":"dim_ab","sink_type":"dim","sink_columns":"id,name,age","sink_pk":"id","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1675066276000,"snapshot":"false","db":"gmall0815_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000011","pos":1087254,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1675066274953,"transaction":null}
    //"op":"d"：{"before":{"source_table":"a","source_type":"ALL","sink_table":"dim_ab","sink_type":"dim","sink_columns":"id,name,age","sink_pk":"id","sink_extend":null},"after":null,"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1675066367000,"snapshot":"false","db":"gmall0815_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000011","pos":1087632,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1675066366022,"transaction":null}
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
        // System.out.println(jsonStr);
        //为了处理方便，将jsonStr转换为jsonObj
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        //获取的对配置表的操作类型
        String op = jsonObj.getString("op");
        //获取广播状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        if ("d".equals(op)) {
            //如果是删除操作，需要将状态中的配置信息删掉
            TableProcess before = jsonObj.getObject("before", TableProcess.class);
            String sourceTable = before.getSourceTable();
            String sinkType = before.getSinkType();
            if ("dim".equals(sinkType)) {
                broadcastState.remove(sourceTable);
                configMap.remove(sourceTable);
            }
        } else {
            //如果是删除外的其它操作，需要将配置信息添加到状态中，或者更新状态中的配置信息
            TableProcess after = jsonObj.getObject("after", TableProcess.class);
            String sinkType = after.getSinkType();
            if ("dim".equals(sinkType)) {
                String sourceTable = after.getSourceTable();
                //获取数仓中维度表表名
                String sinkTable = after.getSinkTable();
                //获取表中字段
                String sinkColumns = after.getSinkColumns();
                //获取建表主键
                String sinkPk = after.getSinkPk();
                //获取建表扩展
                String sinkExtend = after.getSinkExtend();
                //提前将维度表创建出来
                checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
                broadcastState.put(sourceTable, after);
                configMap.put(sourceTable,after);
            }
        }

    }

    //创建维度表
//    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
//        if (sinkPk == null) {
//            sinkPk = "id";
//        }
//        if (sinkExtend == null) {
//            sinkExtend = "";
//        }
//        //拼接建表语句
//        StringBuilder createSql = new StringBuilder("create table if not exists "+ GmallConfig.PHOENIX_SCHEMA +"." + sinkTable + "(");
//        String[] fieldArr = sinkColumns.split(",");
//        for (int i = 0; i < fieldArr.length; i++) {
//            String field = fieldArr[i];
//            if (field.equals(sinkPk)) {
//                createSql.append(field + " varchar primary key");
//            } else {
//                createSql.append(field + " varchar ");
//            }
//            if (i < fieldArr.length - 1) {
//                createSql.append(",");
//            }
//        }
//        createSql.append(") " + sinkExtend);
//        System.out.println("在Phoenix中执行的建表语句：" + createSql);
//
//        // PhoenixUtil.executeSql(createSql.toString());
//        Connection conn = null;
//        PreparedStatement ps = null;
//        try {
//            //获取连接
//            conn = DruidDSUtil.getConnection();
//            //获取数据库操作对象
//            ps = conn.prepareStatement(createSql.toString());
//            //执行SQL
//            ps.execute();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//            //释放资源
//            if(ps != null){
//                try {
//                    ps.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//            if(conn != null){
//                try {
//                    conn.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }

    //创建维度表
    private void checkTable(String tableName, String columnStr, String pk, String ext) {
        //对空值进行处理
        if (ext == null) {
            ext = "";
        }
        if (pk == null) {
            pk = "id";
        }
        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.PHOENIX_SCHEMA + "." + tableName.toUpperCase() + "(");

        String[] columnArr = columnStr.split(",");
        for (int i = 0; i < columnArr.length; i++) {
            String column = columnArr[i];
            if (column.equals(pk)) {
                createSql.append(column + " varchar primary key");
            } else {
                createSql.append(column + " varchar");
            }
            //除了最后一个字段后面不加逗号，其它的字段都需要在后面加一个逗号
            if (i < columnArr.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(") " + ext);
        System.out.println("在phoenix中执行的建表语句: " + createSql);

        PhoenixUtil.executeSql(createSql.toString());

    }

}
