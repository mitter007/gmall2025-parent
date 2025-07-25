package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TradeOrderBean;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * ClassName: DwsTrafficSourceKeywordPageViewWindow
 * Package: com.atguigu.gmall.realtime.dws
 * Description:12.3 从 Kafka 页面日志主题读取数据，统计七日回流用户和当日独立用户数。
 * home detail
 *
 * @Author JWT
 * @Create 2025/7/24 11:10
 * @Version 1.0
 */
public class DwsTradeOrderWindow  extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradePaymentSucWindow()
                .start(1015,
                        4, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS,
                        "dws_trade_order_window");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> KafkaDS) {
        // TODO 将流中数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> mapDS = KafkaDS.map(line -> JSON.parseObject(line));
        mapDS.print();

        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> watermarkDS = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts") * 1000;
            }
        }));
//        {"id":"259172","user_id":"808","sku_id":"6","sku_num":"1","ts":1751014852}


        KeyedStream<JSONObject, String> keyedStream = watermarkDS.keyBy(row -> row.getString("user_id"));
        SingleOutputStreamOperator<TradeOrderBean> processDS = keyedStream.process(new ProcessFunction<JSONObject, TradeOrderBean>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor<>("valueStateDescriptor", String.class);
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, TradeOrderBean>.Context ctx, Collector<TradeOrderBean> out) throws Exception {
                String lastAddCartDate = valueState.value();
                Long ts = value.getLong("ts") * 1000;
                String currVisitDate = DateFormatUtil.tsToDate(ts);
                // 支付成功独立用户数
                Long paymentSucUniqueUserCount = 0L;
                // 支付成功新用户数
                Long paymentSucNewUserCount = 0L;
                if (StringUtils.isEmpty(lastAddCartDate)) {
                    paymentSucUniqueUserCount = 1L;
                    paymentSucNewUserCount = 1L;
                    valueState.update(currVisitDate);
                } else {
                    paymentSucNewUserCount = 0L;
                    if (!lastAddCartDate.equals(currVisitDate)) {
                        paymentSucUniqueUserCount = 1L;
                    }
                    valueState.update(currVisitDate);
                }
                out.collect(new TradeOrderBean("", "", "", paymentSucUniqueUserCount, paymentSucNewUserCount, ts));
            }
        });


        processDS.print();
        // TODO 6.开窗
        AllWindowedStream<TradeOrderBean, TimeWindow> windowDS = processDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        //TODO 7.聚合
        SingleOutputStreamOperator<TradeOrderBean> reduceDS = windowDS.reduce(new ReduceFunction<TradeOrderBean>() {
            @Override
            public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                value1.setPaymentSucNewUserCount(value2.getPaymentSucUniqueUserCount() + value1.getPaymentSucUniqueUserCount());
                value1.setPaymentSucNewUserCount(value2.getPaymentSucNewUserCount() + value1.getPaymentSucNewUserCount());
                return value1;
            }
        }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {

                TradeOrderBean viewBean = values.iterator().next();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDate = DateFormatUtil.tsToDate(window.getStart());
                viewBean.setStt(stt);
                viewBean.setEdt(edt);
                viewBean.setCurDate(curDate);
                out.collect(viewBean);
            }
        });

        //TODO 8.将聚合的结果写到Doris
        reduceDS.print("reduce>>>>");
        reduceDS
                .map(new BeanToJsonStrMapFunction<TradeOrderBean>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_order_window"));

    }
}
