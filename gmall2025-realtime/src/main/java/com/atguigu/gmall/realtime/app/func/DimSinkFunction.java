package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.common.GmallConstant;
import com.atguigu.gmall.realtime.app.utils.PhoenixUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Desc: 将维度流中的数据 写到phoenix表中
 */
public class DimSinkFunction implements SinkFunction<JSONObject> {
    //将流中的数据，保存到phoenix不同的维度表中
    // jonObj:  {"tm_name":"xzls11","sink_table":"dim_base_trademark","id":12}
    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {

        //获取维度输出的目的地表名
        String tableName = jsonObj.getString("sink_table");
        //为了将jsonObj中的所有属性保存到phoenix表中，需要将输出目的地从jsonObj中删除掉
        //===>{"tm_name":"xzls11","id":12}
        jsonObj.remove("sink_table");
        jsonObj.remove("type");

        String upsertSQL = "upsert into " + GmallConstant.PHOENIX_SCHEMA + "." + tableName.toUpperCase()
            + "(" + StringUtils.join(jsonObj.keySet(), ",").toUpperCase() + ") " +
            " values" +
            " ('" + StringUtils.join(jsonObj.values(), "','") + "')";

        //调用向Phoenix表中插入数据的方法
        PhoenixUtil.executeSql(upsertSQL);
    }
}
