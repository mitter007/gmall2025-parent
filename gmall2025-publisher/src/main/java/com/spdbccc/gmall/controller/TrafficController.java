package com.spdbccc.gmall.controller;

import com.spdbccc.gmall.bean.*;
import com.spdbccc.gmall.service.TrafficChannelStatsService;
import com.spdbccc.gmall.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/gmall/realtime/traffic")
public class TrafficController {

    // 自动装载渠道流量统计服务实现类
    @Autowired
    private TrafficChannelStatsService trafficChannelStatsService;

	// 1. 独立访客请求拦截方法
    @RequestMapping("/uvCt")
    public String getUvCt(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateFormatUtil.now();
        }
        List<TrafficUvCt> trafficUvCtList = trafficChannelStatsService.getUvCt(date);
        if (trafficUvCtList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder uvCtValues = new StringBuilder("[");

        for (int i = 0; i < trafficUvCtList.size(); i++) {
            TrafficUvCt trafficUvCt = trafficUvCtList.get(i);
            String ch = trafficUvCt.getCh();
            Integer uvCt = trafficUvCt.getUvCt();

            categories.append("\"").append(ch).append("\"");
            uvCtValues.append("\"").append(uvCt).append("\"");

            if (i < trafficUvCtList.size() - 1) {
                categories.append(",");
                uvCtValues.append(",");
            } else {
                categories.append("]");
                uvCtValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"独立访客数\",\n" +
                "        \"data\": " + uvCtValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

	// 2. 会话数请求拦截方法
    @RequestMapping("/svCt")
    public String getPvCt(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateFormatUtil.now();
        }
        List<TrafficSvCt> trafficSvCtList = trafficChannelStatsService.getSvCt(date);
        if (trafficSvCtList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder svCtValues = new StringBuilder("[");

        for (int i = 0; i < trafficSvCtList.size(); i++) {
            TrafficSvCt trafficSvCt = trafficSvCtList.get(i);
            String ch = trafficSvCt.getCh();
            Integer svCt = trafficSvCt.getSvCt();

            categories.append("\"").append(ch).append("\"");
            svCtValues.append("\"").append(svCt).append("\"");

            if (i < trafficSvCtList.size() - 1) {
                categories.append(",");
                svCtValues.append(",");
            } else {
                categories.append("]");
                svCtValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"会话数\",\n" +
                "        \"data\": " + svCtValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

	// 3. 各会话浏览页面数请求拦截方法
    @RequestMapping("/pvPerSession")
    public String getPvPerSession(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateFormatUtil.now();
        }
        List<TrafficPvPerSession> trafficPvPerSessionList = trafficChannelStatsService.getPvPerSession(date);
        if (trafficPvPerSessionList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder pvPerSessionValues = new StringBuilder("[");

        for (int i = 0; i < trafficPvPerSessionList.size(); i++) {
            TrafficPvPerSession trafficPvPerSession = trafficPvPerSessionList.get(i);
            String ch = trafficPvPerSession.getCh();
            Double pvPerSession = trafficPvPerSession.getPvPerSession();

            categories.append("\"").append(ch).append("\"");
            pvPerSessionValues.append("\"").append(pvPerSession).append("\"");

            if (i < trafficPvPerSessionList.size() - 1) {
                categories.append(",");
                pvPerSessionValues.append(",");
            } else {
                categories.append("]");
                pvPerSessionValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"会话平均页面浏览数\",\n" +
                "        \"data\": " + pvPerSessionValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

	// 4. 各会话累计访问时长请求拦截方法
    @RequestMapping("/durPerSession")
    public String getDurPerSession(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateFormatUtil.now();
        }
        List<TrafficDurPerSession> trafficDurPerSessionList = trafficChannelStatsService.getDurPerSession(date);
        if (trafficDurPerSessionList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder durPerSessionValues = new StringBuilder("[");

        for (int i = 0; i < trafficDurPerSessionList.size(); i++) {
            TrafficDurPerSession trafficDurPerSession = trafficDurPerSessionList.get(i);
            String ch = trafficDurPerSession.getCh();
            Double durPerSession = trafficDurPerSession.getDurPerSession();

            categories.append("\"").append(ch).append("\"");
            durPerSessionValues.append("\"").append(durPerSession).append("\"");

            if (i < trafficDurPerSessionList.size() - 1) {
                categories.append(",");
                durPerSessionValues.append(",");
            } else {
                categories.append("]");
                durPerSessionValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"会话平均页面访问时长\",\n" +
                "        \"data\": " + durPerSessionValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

	// 5. 跳出率请求拦截方法
    @RequestMapping("/ujRate")
    public String getUjRate(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateFormatUtil.now();
        }
        List<TrafficUjRate> trafficUjRateList = trafficChannelStatsService.getUjRate(date);
        if (trafficUjRateList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder ujRateValues = new StringBuilder("[");

        for (int i = 0; i < trafficUjRateList.size(); i++) {
            TrafficUjRate trafficUjRate = trafficUjRateList.get(i);
            String ch = trafficUjRate.getCh();
            Double ujRate = trafficUjRate.getUjRate();

            categories.append("\"").append(ch).append("\"");
            ujRateValues.append("\"").append(ujRate).append("\"");

            if (i < trafficUjRateList.size() - 1) {
                categories.append(",");
                ujRateValues.append(",");
            } else {
                categories.append("]");
                ujRateValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"跳出率\",\n" +
                "        \"data\": " + ujRateValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
        }
}
