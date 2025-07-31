package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


public class HBaseSink implements Sink<Tuple2<JSONObject, TableProcessDim>> {


    @Override
    public SinkWriter<Tuple2<JSONObject, TableProcessDim>> createWriter(InitContext context) throws IOException {
        return new HBaseSinkWriter();
    }

    public static class HBaseSinkWriter implements SinkWriter<Tuple2<JSONObject, TableProcessDim>> {

        private Connection hbaseConn;

//        public HBaseSinkWriter() {
//            try {
//                this.hbaseConn = HBaseUtil.getHBaseConnection();
//                System.out.println("创建连接啦");
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }

        private void initConnectionIfNecessary() throws IOException {
            if (hbaseConn == null) {
                System.out.println("创建连接hbase啦");
                hbaseConn = HBaseUtil.getHBaseConnection();

            }
        }

        @Override
        public void write(Tuple2<JSONObject, TableProcessDim> tuple2, Context context) throws IOException {
            initConnectionIfNecessary();
            // 示例：假设 value 是 "rowkey,column,value"
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

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
//            不做处理
        }

        @Override
        public void close() throws IOException {
            HBaseUtil.closHBaseConnection(hbaseConn);
        }
    }


}
