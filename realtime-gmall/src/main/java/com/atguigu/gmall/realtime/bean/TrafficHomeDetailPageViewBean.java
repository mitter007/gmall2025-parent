package com.atguigu.gmall.realtime.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Felix
 * @date 2024/6/11
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficHomeDetailPageViewBean {
    // 窗口起始时间
    private String stt;
    // 窗口结束时间
    private String edt;
    // 当天日期
    private String cur_date;
    // app 版本号
    @JSONField(serialize = false)  // 要不要序列化这个字段
    private String page;


    // 首页独立访客数
    private Long homeUvCt;
    // 详情页独立访客数
    private Long detailUvCt ;

    // 时间戳
    @JSONField(serialize = false)  // 要不要序列化这个字段
    private Long ts;
}
