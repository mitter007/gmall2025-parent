package com.atguigu.gmall.realtime.app.dws;


import com.atguigu.gmall.realtime.app.bean.KeywordBean;
import com.atguigu.gmall.realtime.app.func.SplitFunction;
import com.atguigu.gmall.realtime.app.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.app.utils.MyKafkaUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Dws01_TrafficKeywordPageViewWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.使用FlinkSQL方式读取 DWD层 页面主题数据,同时提取时间戳生成WaterMark
        tableEnv.executeSql("" +
                "create table page_view( " +
                "    `common` map<string,string>, " +
                "    `page` map<string,string>, " +
                "    `ts` bigint, " +
                "    `rt` as TO_TIMESTAMP_LTZ(ts,3), " +
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_traffic_page_log", "keyword_page_view_221109"));

        //TODO 3.过滤出搜索数据
        Table filterTable = tableEnv.sqlQuery("" +
                "select " +
                "    page['item'] item, " +
                "    rt " +
                "from page_view " +
                "where page['item'] is not null " +
                "and page['last_page_id'] = 'search' " +
                "and page['item_type'] = 'keyword'");
        tableEnv.createTemporaryView("filter_table", filterTable);

        //TODO 4.注册UDTF并分词
        tableEnv.createTemporarySystemFunction("split_func", SplitFunction.class);
        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT " +
                "    word, " +
                "    rt " +
                "FROM filter_table, LATERAL TABLE(split_func(item))");
        tableEnv.createTemporaryView("split_table", splitTable);

        //TODO 5.分组、开窗、聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "SELECT  " +
                "    DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(window_end,'yyyy-MM-dd HH:mm:ss') edt,  " +
                "    word keyword, " +
                "    count(*) keyword_count, " +
                "    UNIX_TIMESTAMP() ts " +
                "FROM TABLE( " +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(rt), INTERVAL '10' seconds)) " +
                "GROUP BY word,window_start,window_end");

        //TODO 6.将动态表转换为流写出到ClickHouse
        DataStream<KeywordBean> rowDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        rowDataStream.print(">>>>>>>");
        rowDataStream.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_keyword_page_view_window values(?,?,?,?,?)"));

        //TODO 7.启动任务
        env.execute("Dws01_TrafficKeywordPageViewWindow");

    }
}
