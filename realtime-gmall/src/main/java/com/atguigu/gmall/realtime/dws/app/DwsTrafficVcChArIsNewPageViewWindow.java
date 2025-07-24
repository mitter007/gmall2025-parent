package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.dws.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.dws.function.BeanToJsonStrMapFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * ClassName: DwsTrafficSourceKeywordPageViewWindow
 * Package: com.atguigu.gmall.realtime.dws
 * Description:12.2 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 * vc-ch-ar-is_new-独立访客数uvCt-会话数svCt-页面浏览数pvCt-累计访问时长durSum
 *
 * @Author JWT
 * @Create 2025/7/24 11:10
 * @Version 1.0
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficVcChArIsNewPageViewWindow()
                .start(1011,
                        4,
                        Constant.TOPIC_DWD_TRAFFIC_PAGE, Constant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> KafkaDS) {
        SingleOutputStreamOperator<JSONObject> map = KafkaDS.map(line -> JSON.parseObject(line));
//        map.print();
        KeyedStream<JSONObject, String> keyedStream = map.keyBy(row -> JSON.parseObject(row.getString("common")).getString("mid"));
//        keyedStream.print("keyedStream");
        SingleOutputStreamOperator<TrafficPageViewBean> processDS = keyedStream.process(new ProcessFunction<JSONObject, TrafficPageViewBean>() {
            private ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
//                状态的ttl
//                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, TrafficPageViewBean>.Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                // 获取上次访问日期
                String lastVisitDate = lastVisitDateState.value();
                Long ts = jsonObject.getLong("ts");
                //获取当前访问日期
                String currVisitDate = DateFormatUtil.tsToDate(ts);
                Long uvct = 0L;
//                每个用户每天访问都会更新一次状态
                if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(currVisitDate)) {
                    uvct = 1L;
                    lastVisitDateState.update(currVisitDate);
                }
                String lastPageId = page.getString("last_page_id");
//                会话数 如果是null 这是新开启的一个会话
                long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;
                out.collect(new TrafficPageViewBean(
                        "",
                        "",
                        "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        uvct,
                        svCt,
                        1L,
                        page.getLong("during_time"),
                        ts
                ));

            }
        });
//        processDS.print("processDS");

        //TODO 4.指定Watermark以及提取事件时间字段
//          这句很简单就是决定什么字段是watermark
        SingleOutputStreamOperator<TrafficPageViewBean> watermarkDS = processDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        //TODO 5.分组--按照统计的维度进行分组 怎么是这样分组的 按照统计粒度分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> tuple4KeyedStream = watermarkDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
//                流量域版本-渠道-地区-访客类别
                        return Tuple4.of(value.getVc(), value.getCh(), value.getAr(), value.getIsNew());
                    }
                });
//        tuple4KeyedStream.print("tuple4KeyedStream>>");
//        TrafficPageViewBean(stt=, edt=, cur_date=, vc=v2.1.134, ch=oppo, ar=13, isNew=0, uvCt=0, svCt=0, pvCt=1, durSum=9540, ts=1749568908082)

//        lambda表达式的写法
//         watermarkDS.keyBy(value -> Tuple4.of(value.getVc(), value.getCh(), value.getAr(), value.getIsNew()));

        //以滚动事件时间窗口为例，分析如下几个窗口相关的问题
        //窗口对象时候创建:当属于这个窗口的第一个元素到来的时候创建窗口对象
        //窗口的起始结束时间（窗口为什么是左闭右开的）
        //向下取整：long start =TimeWindow.getWindowStartWithOffset(timestamp, (globalOffset + staggerOffset) % size, size);
        //窗口什么时候触发计算  window.maxTimestamp() <= ctx.getCurrentWatermark()
        //窗口什么时候关闭     watermark >= window.maxTimestamp() + allowedLateness
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS
                = tuple4KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

//        为什么会开两个窗口
        //TODO 7.聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reduce
                = windowDS.reduce(new ReduceFunction<TrafficPageViewBean>() {
                                      @Override
                                      public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                                          value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                                          value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                                          value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                                          value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                                          return value1;
                                      }
                                  },
//                这一步我就不懂了为什么
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
//                        这里为啥是个迭代器
                        TrafficPageViewBean pageViewBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        pageViewBean.setStt(stt);
                        pageViewBean.setEdt(edt);
                        pageViewBean.setCur_date(curDate);
                        out.collect(pageViewBean);

                    }
                }
        );
//        reduce.print("reduce>>");
        SingleOutputStreamOperator<String> map1 = reduce.map(new MapFunction<TrafficPageViewBean, String>() {
            @Override
            public String map(TrafficPageViewBean value) throws Exception {
                return new BeanToJsonStrMapFunction<TrafficPageViewBean>().map(value);
            }
        });

        map1.print("map1>>");
        map1.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW));

//        reduce.map(bean->(new BeanToJsonStrMapFunction<TrafficPageViewBean>().map(bean)))
    }
}
