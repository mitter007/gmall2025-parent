package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.app.utils.MyKafkaUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 流量域：日志分流   新老访客状态标记修复
 */

/**
 * 新老访客状态标记修复思路
 * 运用Flink状态编程，为每个mid维护一个键控状态，记录首次访问日期。
 * （1）如果is_new的值为1
 * 	如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
 * 	如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
 * 	如果键控状态不为 null，且首次访问日期是当日，说明访问的是新访客，不做操作；
 * （2）如果 is_new 的值为 0
 * 	如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
 * 	如果键控状态不为 null，说明程序已经维护了首次访问日期，不做操作。
 */
public class Dwd01_DwdTrafficBaseLogSplit {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO 2.检查点相关的设置(略)
       
        //TODO 3.从kafka主题中读取日志数据
        //3.1 声明消费的主题以及消费者组
        String topic = "topic_log";
        String groupId = "dwd_traffic_log_split_group";
        //3.2 创建消费者对象
        KafkaSource<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafka source");
        // kafkaStrDS.print(">>>>");

        //TODO 4.简单的ETL以及类型转换   jsonStr->jsonObj  将脏数据放到侧数据中
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {};
        SingleOutputStreamOperator<JSONObject> processDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    //如果在转换的过程中没有发生异常，说明是标准的json字符串，将jsonStr转换为jsonObj，传递到下游
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    out.collect(jsonObj);
                } catch (Exception e) {
                    //如果在转换的过程中发生了异常，说明不是标准的json字符串，属于脏数据，放到侧输出流中
                    ctx.output(dirtyTag, jsonStr);
                }
            }
        });
        //TODO 5.将侧输出流中脏数据写到kafka主题中

//         processDS.print("processDS>>>");
        DataStream<String> dirtyDS = processDS.getSideOutput(dirtyTag);
        // dirtyDS.print("$$$");
        KafkaSink<String> kafkaSink = MyKafkaUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);

        //TODO 6.修复新老访客标记
        //6.1 按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = processDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //6.2 使用Flink的状态编程，进行修复
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
            new RichMapFunction<JSONObject, JSONObject>() {
                //声明状态 用于存放当前设备上次访问日期

//                这是对每一key都维护一个状态吗？ 对的
//                键控状态
//                键控状态，只能应用于KeyedStream的算子中（keyby后的处理算子中）；
//                算子为每一个key绑定一份独立的状态数据；
                private ValueState<String> lastVisitDateState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    //初始化状态
                    ValueStateDescriptor<String> valueStateDescriptor
                        = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                    //valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
//                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        // .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                        .build());
                    this.lastVisitDateState
                        = getRuntimeContext().getState(valueStateDescriptor);
                }

                @Override
                public JSONObject map(JSONObject jsonObj) throws Exception {
                    //获取新老访客标记
                    String isNew = jsonObj.getJSONObject("common").getString("is_new");
                    //获取状态中的上次访问日期
                    String lastVisitDate = lastVisitDateState.value();
                    //获取当前访问日期
                    Long ts = jsonObj.getLong("ts");

                    String curVisitDate = DateFormatUtil.toDate(ts);
                    if ("1".equals(isNew)) {
                        //如果is_new的值为1
                        if (StringUtils.isEmpty(lastVisitDate)) {
                            //如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                            lastVisitDateState.update(curVisitDate);
                        } else {
                            //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                            if (!curVisitDate.equals(lastVisitDate)) {
                                isNew = "0";
                                jsonObj.getJSONObject("common").put("is_new", isNew);
                            }
                        }
                    } else {
                        //如果 is_new 的值为 0
                        //如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。
                        // 当前端新老访客状态标记丢失时，日志进入程序被判定为新访客，
                        // Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。
                        // 本程序选择将状态更新为昨日；
                        if (StringUtils.isEmpty(lastVisitDate)) {
                            String yesterDay = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000);
                            lastVisitDateState.update(yesterDay);
                        }
                    }
                    return jsonObj;
                }
            }
        );
        // fixedDS.print(">>>>");

        //TODO 7.分流  错误日志-错误侧输出流中  启动日志-启动侧输出流中 曝光日志-曝光侧输出流  动作日志-动作侧输出流 页面日志-主流
        //7.1 定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {
        };
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };
        //7.2 分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
            new ProcessFunction<JSONObject, String>() {
                @Override
                public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                    //判断是否是错误日志
                    JSONObject errJsonObj = jsonObj.getJSONObject("err");
                    if (errJsonObj != null) {
                        //将错误日志放到错误侧输出流
                        ctx.output(errTag, jsonObj.toJSONString());
                        jsonObj.remove("err");
                    }
                    //判断是否是启动日志
//                   启动日志里面没有page字段
                    JSONObject startJsonObj = jsonObj.getJSONObject("start");
                    if (startJsonObj != null) {
                        //启动日志  放到启动侧输出流
                        ctx.output(startTag,jsonObj.toJSONString());
                    } else {
                        //页面日志
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        Long ts = jsonObj.getLong("ts");
                        //判断是否有曝光行为
                        JSONArray displayArr = jsonObj.getJSONArray("displays");
                        if(displayArr != null && displayArr.size() > 0){
                            //遍历出所有的曝光信息
                            for (int i = 0; i < displayArr.size(); i++) {
                                //每遍历出一条数据  定义一个新的jsonObj，用于封装曝光数据
                                JSONObject newDisplayJsonObj = new JSONObject();
                                JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                //将曝光信息放到曝光侧输出流中
                                newDisplayJsonObj.put("common",commonJsonObj);
                                newDisplayJsonObj.put("page",pageJsonObj);
                                newDisplayJsonObj.put("display",displayJsonObj);
                                newDisplayJsonObj.put("ts",ts);
                                //将曝光日志放到曝光侧输出流
                                ctx.output(displayTag,newDisplayJsonObj.toJSONString());
                            }
                            jsonObj.remove("displays");
                        }
                        //判断是否有动作
                        JSONArray actionArr = jsonObj.getJSONArray("actions");
                        if(actionArr != null && actionArr.size() > 0){
                            for (int i = 0; i < actionArr.size(); i++) {
                                JSONObject newActionJsonObj = new JSONObject();
                                JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                newActionJsonObj.put("common",commonJsonObj);
                                newActionJsonObj.put("page",pageJsonObj);
                                newActionJsonObj.put("action",actionJsonObj);
                                //将动作日志 放到动作侧输出流
                                ctx.output(actionTag,newActionJsonObj.toJSONString());
                            }
                            jsonObj.remove("actions");
                        }
                        //将页面日志放到主流中
                        out.collect(jsonObj.toJSONString());
                    }
                }
            }
        );
        //TODO 8.将不同流数据写到kafka不同主题中
        DataStream<String> errDS = pageDS.getSideOutput(errTag);
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        pageDS.print(">>>>");
        startDS.print("###");
/*        errDS.print("@@@");
        displayDS.print("$$");
        actionDS.print("&&&&");*/
        KafkaSink<String> kafkaSink1 = MyKafkaUtil.getKafkaSink("dwd_traffic_page_log");

        pageDS.sinkTo(MyKafkaUtil.getKafkaSink("dwd_traffic_page_log"));
        startDS.sinkTo(MyKafkaUtil.getKafkaSink("dwd_traffic_start_log"));
//        errDS.sinkTo(MyKafkaUtil.getKafkaSink("dwd_traffic_err_log"));
//        displayDS.sinkTo(MyKafkaUtil.getKafkaSink("dwd_traffic_display_log"));
//        actionDS.sinkTo(MyKafkaUtil.getKafkaSink("dwd_traffic_action_log"));

        env.execute();
    }
}
