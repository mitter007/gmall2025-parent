package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.dws.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.dws.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.dws.function.BeanToJsonStrMapFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;


/**
 * ClassName: DwsTrafficSourceKeywordPageViewWindow
 * Package: com.atguigu.gmall.realtime.dws
 * Description:12.3 流量域首页、详情页页面浏览各窗口汇总表
 * home detail
 *
 * @Author JWT
 * @Create 2025/7/24 11:10
 * @Version 1.0
 */
public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficHomeDetailPageViewWindow()
                .start(1012,
                        4,
                        Constant.TOPIC_DWD_TRAFFIC_PAGE, Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW);
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
        map.print("map>>>>>");
        SingleOutputStreamOperator<JSONObject> filterDS = map.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                if (pageId.equals("home") || pageId.equals("good_detail")) {
                    return true;
                }
                return false;
            }
        });

        filterDS.print("filterDS>>>>>");
        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> watermarkDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));


        KeyedStream<JSONObject, String> keyedStream = watermarkDS.keyBy(row -> JSON.parseObject(row.getString("common")).getString("mid"));
        keyedStream.print("keyedStream>>>>");
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanDS = keyedStream.process(new ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            private MapState<String, String> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {


                MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, String.class);
                mapStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                mapState = getRuntimeContext().getMapState(mapStateDescriptor);

            }

            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
//                mapState.
                String homeStartdate = mapState.get("homeStartdate");
                String detailStartdate = mapState.get("detailStartdate");
                Long ts = jsonObject.getLong("ts");
                //获取当前访问日期
                String currVisitDate = DateFormatUtil.tsToDate(ts);
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                Long homeUvCt = 0L;
                Long detailUvCt = 0L;
                if (pageId.equals("home")) {
                    if (StringUtils.isEmpty(homeStartdate) || currVisitDate.equals(homeStartdate)) {
                        homeUvCt = 1L;
                        mapState.put("homeStartdate", currVisitDate);
                    }
                }
                if (pageId.equals("good_detail")) {
                    if (StringUtils.isEmpty(detailStartdate) || !currVisitDate.equals(detailStartdate)) {
                        detailUvCt = 1L;
                        mapState.put("detailStartdate", currVisitDate);
                    }
                }
                if (homeUvCt != 0L || detailUvCt != 0L) {
                    TrafficHomeDetailPageViewBean bean = new TrafficHomeDetailPageViewBean();
                    bean.setDetailUvCt(detailUvCt);
                    bean.setHomeUvCt(homeUvCt);
                    bean.setPage(pageId);
//                    bean.setCur_date(currVisitDate);
                    bean.setTs(ts);
                    out.collect(bean);

                }

            }
        });

        beanDS.print("beanDS>>>");

        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS = beanDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        //TODO 7.聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setDetailUvCt(value1.getDetailUvCt() + value2.getDetailUvCt());
                        return value1;
                    }
                },
                new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        TrafficHomeDetailPageViewBean viewBean = values.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        viewBean.setStt(stt);
                        viewBean.setEdt(edt);
                        viewBean.setCur_date(curDate);
                        out.collect(viewBean);
                    }
                }
        );
        //TODO 8.将聚合的结果写到Doris
        reduceDS.print("reduce>>>>");
        reduceDS
                .map(new BeanToJsonStrMapFunction<TrafficHomeDetailPageViewBean>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_home_detail_page_view_window"));
/*
        //TODO 5.分组--按照统计的维度进行分组 怎么是这样分组的 按照统计粒度分组
        KeyedStream<TrafficHomeDetailPageViewBean, String> keyedDS = beanDS.keyBy(TrafficHomeDetailPageViewBean::getPage);

//        TODO 开窗
        WindowedStream<TrafficHomeDetailPageViewBean, String, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduce = windowDS.reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                value1.setDetailUvCt(value1.getDetailUvCt() + value2.getDetailUvCt());
                return value1;
            }
        }, new WindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> input, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                //                        这里为啥是个迭代器
                TrafficHomeDetailPageViewBean homeDetailPageViewBean = input.iterator().next();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDate = DateFormatUtil.tsToDate(window.getStart());
                homeDetailPageViewBean.setStt(stt);
                homeDetailPageViewBean.setEdt(edt);
                homeDetailPageViewBean.setCur_date(curDate);
                out.collect(homeDetailPageViewBean);
            }
        });

        reduce.print("reduce>>>>>>");

//        为什么会开两个窗口
        //TODO 7.聚合计算

//        reduce.print("reduce>>");
        SingleOutputStreamOperator<String> map1 = reduce.map(new MapFunction<TrafficHomeDetailPageViewBean, String>() {
            @Override
            public String map(TrafficHomeDetailPageViewBean value) throws Exception {
                return new BeanToJsonStrMapFunction<TrafficHomeDetailPageViewBean>().map(value);
            }
        });

        map1.print("map1>>");
        map1.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW));
*/

//        reduce.map(bean->(new BeanToJsonStrMapFunction<TrafficPageViewBean>().map(bean)))
    }
}
