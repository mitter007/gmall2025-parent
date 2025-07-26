package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.CartAddUuBean;
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
public class DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeCartAddUuWindow()
                .start(1014,
                        4, Constant.TOPIC_DWD_TRADE_CART_ADD,
                        "dws_trade_cart_add_uu_window");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> KafkaDS) {
        // TODO 将流中数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> mapDS = KafkaDS.map(line -> JSON.parseObject(line));
        mapDS.print();
        ;
        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> watermarkDS = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts")*1000;
            }
        }));
//        {"id":"259172","user_id":"808","sku_id":"6","sku_num":"1","ts":1751014852}


        KeyedStream<JSONObject, String> keyedStream = watermarkDS.keyBy(row -> row.getString("user_id"));
        SingleOutputStreamOperator<CartAddUuBean> processDS = keyedStream.process(new ProcessFunction<JSONObject, CartAddUuBean>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor<>("valueStateDescriptor", String.class);
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, CartAddUuBean>.Context ctx, Collector<CartAddUuBean> out) throws Exception {
                String lastAddCartDate = valueState.value();
                String currVisitDate = DateFormatUtil.tsToDate(value.getLong("ts")*1000);
                Long cartAddUuCt = 0L;
                if (StringUtils.isEmpty(lastAddCartDate) || lastAddCartDate != currVisitDate) {
                    cartAddUuCt = 1L;
                    out.collect(new CartAddUuBean("", "", "", cartAddUuCt));
                    valueState.update(currVisitDate);
                }
            }
        });
        processDS.print();
        // TODO 6.开窗
        AllWindowedStream<CartAddUuBean, TimeWindow> windowDS = processDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        //TODO 7.聚合
        SingleOutputStreamOperator<CartAddUuBean> reduceDS = windowDS.reduce(new ReduceFunction<CartAddUuBean>() {
            @Override
            public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                return value1;
            }
        }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {

                CartAddUuBean viewBean = values.iterator().next();
                System.out.println(viewBean);
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
                .map(new BeanToJsonStrMapFunction<CartAddUuBean>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));

    }
}
