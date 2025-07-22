package com.atguigu.gmall.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.KafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

/**
 * ClassName: DwdBaseLog
 * Package: com.atguigu.gmall.realtime.dwd
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/21 16:45
 * @Version 1.0
 */
public class DwdBaseLog extends BaseApp {

    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";

    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(
                1002,
                4, Constant.TOPIC_LOG
                ,
                "dwd_base_log"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        SingleOutputStreamOperator<JSONObject> JsonDS = etl(kafkaDS);
        SingleOutputStreamOperator<JSONObject> fixedDS = fixedNewAndOld(JsonDS);
        HashMap<String, DataStream<String>> map = splitStream(fixedDS);
        sinkToKafka(map);
    }


    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaDS) {
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };
        SingleOutputStreamOperator<JSONObject> process = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {

                    ctx.output(dirtyTag, value);
                }

            }
        });
        return process;


    }

    //    {"actions":[{"action_id":"favor_add","item":"28","item_type":"sku_id","ts":1749489538572},{"action_id":"cart_add","item":"28","item_type":"sku_id","ts":1749489541572}],"common":{"ar":"21","ba":"vivo","ch":"xiaomi","is_new":"1","md":"vivo x90","mid":"mid_205","os":"Android 13.0","sid":"6217a3b7-128e-45c6-9fca-c7a8146e66a3","uid":"482","vc":"v2.0.1"},"displays":[{"item":"9","item_type":"sku_id","pos_id":4,"pos_seq":0},{"item":"32","item_type":"sku_id","pos_id":4,"pos_seq":1},{"item":"30","item_type":"sku_id","pos_id":4,"pos_seq":2}],"page":{"during_time":16589,"from_pos_id":4,"from_pos_seq":3,"item":"28","item_type":"sku_id","last_page_id":"good_detail","page_id":"good_detail"},"ts":1749489536572}
    private static SingleOutputStreamOperator<JSONObject> fixedNewAndOld(SingleOutputStreamOperator<JSONObject> JsonDS) {
//        按照mid对设备id进行分组
        KeyedStream<JSONObject, String> jsonObjectKeyedDS = JsonDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {

                String key = jsonObject.getJSONObject("common").getString("mid");
                return key;
            }
        });

        SingleOutputStreamOperator<JSONObject> processDS = jsonObjectKeyedDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", TypeInformation.of(String.class));
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

//                获取is_new的值
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                String lastvisitDate = lastVisitDateState.value();
                Long ts = jsonObject.getLong("ts");
                String currVisitDate = DateFormatUtil.tsToDate(ts);
                if ("1".equals(isNew)) {
                    if (StringUtils.isEmpty(lastvisitDate)) {
                        //如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                        lastVisitDateState.update(currVisitDate);
                    } else {
                        //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                        if (!currVisitDate.equals(lastvisitDate)) {
                            isNew = "0";
                            jsonObject.getJSONObject("common").put("is_new", isNew);
                        }
                    }
                } else {
                    if (StringUtils.isEmpty(lastvisitDate)) {
                        //如果 is_new 的值为 0
                        //	如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，
                        // 日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                        String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                        lastVisitDateState.update(yesterDay);

                    }
                }
                out.collect(jsonObject);
            }
        });
        return processDS;

    }

    private HashMap<String, DataStream<String>> splitStream(SingleOutputStreamOperator<JSONObject> fixedDS) {
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> errTag = new OutputTag<String>("err") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("action") {
        };
//"{\"actions\":[{\"action_id\":\"favor_add\",\"item\":\"28\",\"item_type\":\"sku_id\",\"ts\":1749489538572},{\"action_id\":\"cart_add\",\"item\":\"28\",\"item_type\":\"sku_id\",\"ts\":1749489541572}],\"common\":{\"ar\":\"21\",\"ba\":\"vivo\",\"ch\":\"xiaomi\",\"is_new\":\"1\",\"md\":\"vivo x90\",\"mid\":\"mid_205\",\"os\":\"Android 13.0\",\"sid\":\"6217a3b7-128e-45c6-9fca-c7a8146e66a3\",\"uid\":\"482\",\"vc\":\"v2.0.1\"},\"displays\":[{\"item\":\"9\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":0},{\"item\":\"32\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":1},{\"item\":\"30\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":2}],\"page\":{\"during_time\":16589,\"from_pos_id\":4,\"from_pos_seq\":3,\"item\":\"28\",\"item_type\":\"sku_id\",\"last_page_id\":\"good_detail\",\"page_id\":\"good_detail\"},\"ts\":1749489536572}"

        SingleOutputStreamOperator<String> processDS = fixedDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                JSONObject err = jsonObject.getJSONObject("err");
                if (err != null) {
                    ctx.output(errTag, jsonObject.toJSONString());
                    jsonObject.remove("err");
                }
                JSONObject start = jsonObject.getJSONObject("start");
                if (start != null) {
                    //~~~启动日志~~~
                    //将启动日志写到启动侧输出流
                    ctx.output(startTag, jsonObject.toJSONString());
                } else {
//                    页面日志
                    JSONObject commonJson = jsonObject.getJSONObject("common");
                    JSONObject pageJson = jsonObject.getJSONObject("page");
                    Long ts = jsonObject.getLong("ts");
//                    曝光日志
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
//                        遍历当前页面的所有曝光信息
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displaysJSONObject = displays.getJSONObject(i);
//                           定义一个新的JSON对象，用于封装遍历出来的曝光信息
                            JSONObject jsonObj = new JSONObject();
                            jsonObj.put("common", commonJson);
                            jsonObj.put("page", pageJson);
                            jsonObj.put("display", displaysJSONObject);
                            jsonObj.put("ts", ts);
                            ctx.output(displayTag, jsonObj.toJSONString());
                        }
                        jsonObject.remove("displays");
                    }
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject actionsJSONObject = actions.getJSONObject(i);
                            JSONObject jsonObj = new JSONObject();
                            jsonObj.put("common", commonJson);
                            jsonObj.put("page", pageJson);
                            jsonObj.put("action", actionsJSONObject);
                            jsonObj.put("ts", ts);
                            ctx.output(actionTag, jsonObj.toJSONString());
                        }
                        jsonObject.remove("actions");
                    }
                }
                out.collect(jsonObject.toJSONString());

            }
        });
        SideOutputDataStream<String> errTagOutput = processDS.getSideOutput(errTag);
        SideOutputDataStream<String> startTagOutput = processDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayTagOutput = processDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionTagOutput = processDS.getSideOutput(actionTag);
        errTagOutput.print("1>>");
        startTagOutput.print("2>>");
        displayTagOutput.print("3>>");
        actionTagOutput.print("4>>");
        processDS.print("5>>");



        HashMap<String, DataStream<String>> map = new HashMap<>();
        map.put(Constant.TOPIC_DWD_TRAFFIC_ERR, errTagOutput);
        map.put(Constant.TOPIC_DWD_TRAFFIC_START, startTagOutput);
        map.put(Constant.TOPIC_DWD_TRAFFIC_DISPLAY, displayTagOutput);
        map.put(Constant.TOPIC_DWD_TRAFFIC_ACTION, actionTagOutput);
        map.put(Constant.TOPIC_DWD_TRAFFIC_PAGE, processDS);
        return map;
    }

    private void sinkToKafka(HashMap<String, DataStream<String>> map) {
//        Set<Map.Entry<String, DataStream<String>>> entries = map.entrySet();
//        for (Map.Entry<String, DataStream<String>> entry : entries) {
//            DataStream<String> value = entry.getValue();
//            value.sinkTo(KafkaUtil.getKafkaSink(Constant.KAFKA_BROKERS, entry.getKey()));
//        }


        map
                .get(Constant.TOPIC_DWD_TRAFFIC_PAGE)
                .sinkTo(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        map
                .get(Constant.TOPIC_DWD_TRAFFIC_ERR)
                .sinkTo(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        map
                .get(Constant.TOPIC_DWD_TRAFFIC_START)
                .sinkTo(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        map
                .get(Constant.TOPIC_DWD_TRAFFIC_DISPLAY)
                .sinkTo(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        map
                .get(Constant.TOPIC_DWD_TRAFFIC_ACTION)
                .sinkTo(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }


}
