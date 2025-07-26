package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
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
public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeProvinceOrderWindow().start(
                1018,
                4,
                "dwd_trade_order_detail",
                "dws_trade_province_order_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        //TODO 1.过滤空消息  并对流中数据进行类型转换    jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (value != null) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                }
            }
        });
        jsonDS.print("json>>");
        //TODO 2.按照唯一键(订单明细的id)进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonDS.keyBy(jsonObject -> jsonObject.getString("id"));
        //TODO 3.使用状态进行去重
        SingleOutputStreamOperator<JSONObject> processDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> lastJsonObjState;

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("valueStateDescriptor", JSONObject.class);
//                为什么有时候状态需要设置过期时间，有些时候不需要呢
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                JSONObject lastJsonObj = lastJsonObjState.value();
                if (lastJsonObj != null) {
                    //说明重复了 ，将已经发送到下游的数据(状态)，影响到度量值的字段进行取反再传递到下游
                    String splitOriginalAmount = lastJsonObj.getString("split_original_amount");
                    String splitCouponAmount = lastJsonObj.getString("split_coupon_amount");
                    String splitActivityAmount = lastJsonObj.getString("split_activity_amount");
                    String splitTotalAmount = lastJsonObj.getString("split_total_amount");

                    lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                    lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                    lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
                    lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                    out.collect(lastJsonObj);
                }
                lastJsonObjState.update(jsonObject);
                out.collect(jsonObject);


            }
        });

        SingleOutputStreamOperator<JSONObject> watermarkDS = processDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts") * 1000;
            }
        }));
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = watermarkDS.map(new MapFunction<JSONObject, TradeProvinceOrderBean>() {
            @Override
            public TradeProvinceOrderBean map(JSONObject jsonObj) throws Exception {
                String provinceId = jsonObj.getString("province_id");
                BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                Long ts = jsonObj.getLong("ts") * 1000;
                TradeProvinceOrderBean orderBean = TradeProvinceOrderBean.builder()
                        .provinceId(provinceId)
                        .orderCount(1L)
                        .orderAmount(splitTotalAmount)
                        .ts(ts)
                        .build();
                return orderBean;
            }
        });

        beanDS.print("bean>>");
//        按skuid分组
        KeyedStream<TradeProvinceOrderBean, String> keyByDS = beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);

//        开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = keyByDS.window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

//       聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount()+ value2.getOrderCount());
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                }
                , new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderBean> elements, Collector<TradeProvinceOrderBean> out) throws Exception {
                        TradeProvinceOrderBean orderBean = elements.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        out.collect(orderBean);
                    }
                });

        reduceDS.print("reduce>>>");
        //TODO 9.关联省份维度
        //异步IO + 模板
        SingleOutputStreamOperator<TradeProvinceOrderBean> withDS = AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<TradeProvinceOrderBean>() {
            @Override
            public void addDims(TradeProvinceOrderBean orderBean, JSONObject dimJsonObj) {
                orderBean.setProvinceName(dimJsonObj.getString("name"));

            }

            @Override
            public String getTableName() {
                return "dim_base_province";
            }

            @Override
            public String getRowKey(TradeProvinceOrderBean orderBean) {
                return orderBean.getProvinceId();
            }
        }, 60, TimeUnit.SECONDS);


        withDS.print("withDS>>");
        withDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));
    }
}