package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: DwsTradeSkuOrderWindowSyncCache
 * Package: com.atguigu.gmall.realtime.dws.app
 * Description: 12.9 交易域SKU粒度下单各窗口汇总表
 *
 * @Author JWT
 * @Create 2025/7/25 21:28
 * @Version 1.0
 */
public class DwsTradeTrademarkCategoryUserRefundWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeTrademarkCategoryUserRefundWindow().start(
                1019,
                4,
                "dwd_trade_order_refund",
                "dws_trade_trademark_category_user_refund_window2"
        );
    }

    //    {"id":"190","user_id":"117","order_id":"1743","sku_id":"15","province_id":"16","date_id":"2025-06-11","create_time":"2025-06-11 18:22:57","refund_type_code":"1502","refund_type_name":"退货退款","refund_reason_type_code":"1303","refund_reason_type_name":"缺货","refund_reason_txt":"退款原因具体：5445135091","refund_num":"1","refund_amount":"9799.0","ts":1753323089}
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        //TODO 1.过滤空消息  并对流中数据进行类型转换    jsonStr->jsonObj
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanDS = kafkaDS.process(new ProcessFunction<String, TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public void processElement(String value, ProcessFunction<String, TradeTrademarkCategoryUserRefundBean>.Context ctx, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                if (value != null) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    TradeTrademarkCategoryUserRefundBean refundBean = TradeTrademarkCategoryUserRefundBean.builder()
                            .orderIdSet(new HashSet<>(Collections.singleton(jsonObject.getString("order_id"))))
                            .ts(jsonObject.getLong("ts") * 1000)
                            .userId(jsonObject.getString("user_id"))
                            .skuId(jsonObject.getString("sku_id"))
                            .build();

                    out.collect(refundBean);
                }
            }
        });
        beanDS.print("json>>");
        //TODO 2.关联sku维度
        //异步IO + 模板
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withSkuInfoDS = AsyncDataStream.unorderedWait(beanDS, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public void addDims(TradeTrademarkCategoryUserRefundBean orderBean, JSONObject dimJsonObj) {
                orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
            }

            @Override
            public String getTableName() {
                return "dim_sku_info";
            }

            @Override
            public String getRowKey(TradeTrademarkCategoryUserRefundBean orderBean) {
                return orderBean.getSkuId();
            }
        }, 60, TimeUnit.SECONDS);
        withSkuInfoDS.print("withSkuInfoDS>>>");
        //TODO 3.设置水位线
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> watermarkDS = withSkuInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public long extractTimestamp(TradeTrademarkCategoryUserRefundBean element, long recordTimestamp) {
                System.out.println("**************3*********");
                return element.getTs();
            }
        }).withIdleness(Duration.ofSeconds(120L)));
//        TODO 4.分组，开窗，聚合
        KeyedStream<TradeTrademarkCategoryUserRefundBean, String> keyedStream = watermarkDS.keyBy(bean -> bean.getUserId() + "_" + bean.getCategory3Id() + "_" + bean.getTrademarkId());
//          开窗
        WindowedStream<TradeTrademarkCategoryUserRefundBean, String, TimeWindow> windowDS = keyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)));
//          聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceDS = windowDS.reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                System.out.println("**************2*********");
                return value1;
            }
        }, new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
            @Override
            public void process(String stringStringTuple3, ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>.Context context, Iterable<TradeTrademarkCategoryUserRefundBean> elements, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                TradeTrademarkCategoryUserRefundBean orderBean = elements.iterator().next();
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDate = DateFormatUtil.tsToDate(window.getStart());
                orderBean.setStt(stt);
                orderBean.setEdt(edt);
                orderBean.setCurDate(curDate);
                orderBean.setRefundCount((long) orderBean.getOrderIdSet().size());
                out.collect(orderBean);
            }
        });
        reduceDS.print("reduce>>>");

        //TODO 9.（1）关联base_trademark表
        //异步IO + 模板
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withTrademarkDS = AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public void addDims(TradeTrademarkCategoryUserRefundBean orderBean, JSONObject dimJsonObj) {
                orderBean.setTrademarkName(dimJsonObj.getString("tm_name"));
            }

            @Override
            public String getTableName() {
                return "dim_base_trademark";
            }

            @Override
            public String getRowKey(TradeTrademarkCategoryUserRefundBean orderBean) {
                return orderBean.getTrademarkId();
            }
        }, 60, TimeUnit.SECONDS);
        withTrademarkDS.print("withTrademarkDS>>>");

        //TODO 12.关联category3维度
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c3Stream = AsyncDataStream.unorderedWait(
                withTrademarkDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setCategory3Name(dim.getString("name"));
                        bean.setCategory2Id(dim.getString("category2_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        c3Stream.print("c3Stream>>>");
        //TODO 13.关联category2维度

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c2Stream = AsyncDataStream.unorderedWait(
                c3Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setCategory2Name(dim.getString("name"));
                        bean.setCategory1Id(dim.getString("category1_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        c2Stream.print("c2Stream>>>");
        //TODO 14.关联category1维度

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withC1DS = AsyncDataStream.unorderedWait(
                c2Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setCategory1Name(dim.getString("name"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        withC1DS.print("withC1DS>>");
        withC1DS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_trademark_category_user_refund_window"));
    }
}