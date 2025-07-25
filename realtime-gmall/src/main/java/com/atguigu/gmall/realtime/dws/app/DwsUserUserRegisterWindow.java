package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.UserLoginBean;
import com.atguigu.gmall.realtime.bean.UserRegisterBean;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
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
public class DwsUserUserRegisterWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsUserUserRegisterWindow()
                .start(1014,
                        4, Constant.TOPIC_DWD_USER_REGISTER,
                        "dws_user_user_register_window");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> KafkaDS) {
        SingleOutputStreamOperator<JSONObject> mapDS = KafkaDS.map(line -> JSON.parseObject(line));
        mapDS.print();;
        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> watermarkDS = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {

                String createTime = element.getString("create_time");
                Long ts = DateFormatUtil.dateTimeToTs(createTime);
                return ts;
            }
        }));

//        {"create_time":"2025-07-01 23:07:00","id":2310,"type":"insert"}
        SingleOutputStreamOperator<UserRegisterBean> processDS = watermarkDS.process(new ProcessFunction<JSONObject, UserRegisterBean>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, UserRegisterBean>.Context ctx, Collector<UserRegisterBean> out) throws Exception {
                Long reguCt = 0L;
                String createTime = jsonObject.getString("create_time");
                Long ts = DateFormatUtil.dateTimeToTs(createTime);
                if ("insert".equals(jsonObject.getString("type"))) {
                    reguCt = 1L;
                    out.collect(new UserRegisterBean("", "", "", reguCt, ts));
                }

            }
        });
        processDS.print();

        // TODO 6.开窗
        AllWindowedStream<UserRegisterBean, TimeWindow> windowDS = processDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        //TODO 7.聚合
        SingleOutputStreamOperator<UserRegisterBean> reduceDS = windowDS.reduce(new ReduceFunction<UserRegisterBean>() {
            @Override
            public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                value1.setReguCt(value1.getReguCt() + value2.getReguCt());

                return value1;
            }
        }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {
                UserRegisterBean viewBean = values.iterator().next();
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
                .map(new BeanToJsonStrMapFunction<UserRegisterBean>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_user_user_register_window"));

    }
}
