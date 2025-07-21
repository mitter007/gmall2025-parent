package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * ClassName: HbaseUtil
 * Package: com.atguigu.gmall.realtime.common.util
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/20 21:37
 * @Version 1.0
 */
public class HbaseUtil {

    private static final Logger logger = LogManager.getLogger(HbaseUtil.class);

    public static Connection getHbaseconnection() throws IOException {

        Configuration config = HBaseConfiguration.create();

        // 设置 ZooKeeper 的地址和端口
        config.set("hbase.zookeeper.quorum", Constant.HBASE_HOST); // 多个用逗号分隔
        config.set("hbase.zookeeper.property.clientPort", "2181");

        // HBase master 端口、路径（可选）
        // config.set("hbase.master", "hostname:16000");

        Connection connection = null;

        // 2. 创建连接
        // TODO: 进行 HBase 操作，比如获取表、写入数据等
        connection = ConnectionFactory.createConnection(config);
        return connection;
    }

    public static void closeConnection(Connection connection) {

        // 3. 关闭连接
        if (connection != null) {
            try {
                connection.close();
                logger.info("HBase 连接已关闭");
            } catch (Exception e) {
                logger.error("HBase 链接关闭异常");
                e.printStackTrace();
            }
        }

    }

    //建表
    public static void createHBaseTable(Connection hbaseConn, String namespace, String tableName, String... families) {
        if (families.length < 1) {
            System.out.println("至少需要一个列族");
            return;
        }

        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tableNameObj)) {
                System.out.println("表空间" + namespace + "下的表" + tableName + "已存在");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }

            admin.createTable(tableDescriptorBuilder.build());

            System.out.println("表空间" + namespace + "下的表" + tableName + "创建成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //删除表
    public static void dropHBaseTable(Connection hbaseConn, String namespace, String tableName) {

        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            //判断要删除的表是否存在
            if (!admin.tableExists(tableNameObj)) {
                System.out.println("要删除的表空间" + namespace + "下的表" + tableName + "不存在");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("删除的表空间" + namespace + "下的表" + tableName + "");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 向表中put数据
     *
     * @param hbaseConn 连接对象
     * @param namespace 表空间
     * @param tableName 表名
     * @param rowKey    rowkey
     * @param family    列族
     * @param jsonObj   要put的数据
     */
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObj) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columns = jsonObj.keySet();
            for (String column : columns) {
                String value = jsonObj.getString(column);
                if (StringUtils.isNotEmpty(value)) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("向表空间" + namespace + "下的表" + tableName + "中put数据" + rowKey + "成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //从表中删除数据
    public static void delRow(Connection hbaseConn, String namespace, String tableName, String rowKey) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("从表空间" + namespace + "下的表" + tableName + "中删除数据" + rowKey + "成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据rowkey从Hbase表中查询一行数据
     *
     * @param hbaseConn          hbase连接对象
     * @param namespace          表空间
     * @param tableName          表名
     * @param rowKey             rowkey
     * @param clz                将查询的一行数据 封装的类型
     * @param isUnderlineToCamel 是否将下划线转换为驼峰命名
     * @param <T>
     * @return
     */
    public static <T> T getRow(Connection hbaseConn, String namespace, String tableName, String rowKey, Class<T> clz, boolean... isUnderlineToCamel) {
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            if (cells != null && cells.size() > 0) {
                //定义一个对象，用于封装查询出来的一行数据
                T obj = clz.newInstance();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if (defaultIsUToC) {
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                return obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * 以异步的方式 从HBase维度表中查询维度数据
     *
     * @param asyncConn 异步操作HBase的连接
     * @param namespace 表空间
     * @param tableName 表名
     * @param rowKey    rowkey
     * @return
     */
    public static JSONObject readDimAsync(AsyncConnection asyncConn, String namespace, String tableName, String rowKey) {
        try {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncConn.getTable(tableNameObj);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if (cells != null && cells.size() > 0) {
                JSONObject jsonObj = new JSONObject();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObj.put(columnName, columnValue);
                }
                return jsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        Connection hBaseConnection = getHbaseconnection();
        JSONObject jsonObj = getRow(hBaseConnection, Constant.HBASE_NAMESPACE, "dim_base_trademark", "1", JSONObject.class);
        System.out.println(jsonObj);
        closeConnection(hBaseConnection);
    }
}
