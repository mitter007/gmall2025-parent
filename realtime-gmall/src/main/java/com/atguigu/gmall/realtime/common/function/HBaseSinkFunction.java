package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * ClassName: HBaseSinkFunction
 * Package: com.atguigu.gmall.realtime.common.function
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/21 11:01
 * @Version 1.0
 */
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hbaseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHBaseConnection();

    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closHBaseConnection(hbaseConn);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> tuple2, Context context) {
        JSONObject f0 = tuple2.f0;
        TableProcessDim f1 = tuple2.f1;
        String type = f0.getString("type");
        f0.remove(type);
        String sinkTable = f1.getSinkTable();
        String rowkey = f0.getString(f1.getSinkRowKey());

        if ("delete".equals(type)) {
            //从业务数据库维度表中做了删除操作  需要将HBase维度表中对应的记录也删除掉
            HBaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowkey);
        } else {
            //如果不是delete，可能的类型有insert、update、bootstrap-insert，上述操作对应的都是向HBase表中put数据
            String sinkFamily = f1.getSinkFamily();
            HBaseUtil.putRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowkey, sinkFamily, f0);
        }

    }
}
