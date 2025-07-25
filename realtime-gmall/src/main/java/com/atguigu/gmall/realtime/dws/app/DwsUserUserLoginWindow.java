package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.UserLoginBean;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
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
public class DwsUserUserLoginWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsUserUserLoginWindow()
                .start(1013,
                        4, Constant.TOPIC_DWD_TRAFFIC_PAGE,
                        "dws_user_user_login_window");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> KafkaDS) {
        SingleOutputStreamOperator<JSONObject> map = KafkaDS.map(line -> JSON.parseObject(line));
//        map.print();
//        keyedStream.print("keyedStream");
/*        SingleOutputStreamOperator<JSONObject> processDS = map.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                if (pageId.equals("home ")||pageId.equals("good_detail")){
                    out.collect(jsonObject);
                }
            }
        });*/
//        map.print("map>>>>>");
        SingleOutputStreamOperator<JSONObject> filterDS = map.filter(new FilterFunction<JSONObject>() {
//            直接使用stiing的isempty会报错 空指针异常，应该用stringutils.isempty方法
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                boolean flag = jsonObject.getJSONObject("common").getString("uid") != null && (jsonObject.getJSONObject("page").getString("last_page_id") == null || jsonObject.getJSONObject("page").getString("last_page_id").equals("login"));
                return flag;
            }
        });

//        filterDS.print("filterDS>>>>>");
        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> watermarkDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        KeyedStream<JSONObject, String> keyedStream = watermarkDS.keyBy(row -> JSON.parseObject(row.getString("common")).getString("uid"));
        keyedStream.print("keyedStream>>>>");
        SingleOutputStreamOperator<UserLoginBean> beanDS = keyedStream.process(new ProcessFunction<JSONObject, UserLoginBean>() {
            private ValueState<String> lastLoginDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
//                这次并乜有设置状态的生命周期诶
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
//
                lastLoginDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                String lastLoginDate = lastLoginDateState.value();
                Long ts = jsonObject.getLong("ts");

                //获取当前访问日期
                String currVisitDate = DateFormatUtil.tsToDate(ts);
                Long uuCt = 0L;
                Long backCt = 0L;

                if (lastLoginDate != null) {
                    if (!lastLoginDate.equals(currVisitDate)) {
                        uuCt = 1L;
                        lastLoginDateState.update(currVisitDate);
                        if ((Integer.getInteger(currVisitDate) - Integer.getInteger(lastLoginDate)) >= 8) {
                            backCt = 1L;
                        }
                        out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                    }
                } else {
                    uuCt = 1L;
                    backCt = 0L;
                    lastLoginDateState.update(currVisitDate);
                    out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                }
            }
        });

        beanDS.print("beanDS>>>");

        AllWindowedStream<UserLoginBean, TimeWindow> windowDS = beanDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(20)));
        //TODO 7.聚合
        SingleOutputStreamOperator<UserLoginBean> reduceDS = windowDS.reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                },
                new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                        UserLoginBean viewBean = values.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        viewBean.setStt(stt);
                        viewBean.setEdt(edt);
                        viewBean.setCurDate(curDate);
                        out.collect(viewBean);
                    }
                }
        );
        //TODO 8.将聚合的结果写到Doris
        reduceDS.print("reduce>>>>");
        reduceDS
                .map(new BeanToJsonStrMapFunction<UserLoginBean>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_user_user_login_window"));

    }
}
