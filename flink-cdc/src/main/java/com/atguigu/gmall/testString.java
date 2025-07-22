package com.atguigu.gmall;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ClassName: testString
 * Package: com.atguigu.gmall
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/22 9:49
 * @Version 1.0
 */
public class testString {
    public static void main(String[] args) {
        String a="123";
        String b="aaa";
        swit(a,b);
        System.out.println(a);//123
        System.out.println(b);//aaa
        String json="{\"entry\":\"install\",\"loading_time\":3324,\"open_ad_id\":16,\"open_ad_ms\":2554,\"open_ad_skip_ms\":95653}";
        JSONObject jsonObject = JSON.parseObject(json);
        deleteNotNeedColumns(jsonObject,"entry,open_ad_ms");
        System.out.println(jsonObject);//{"entry":"install","open_ad_ms":2554}


    }

    private static void swit(String a,String b){
        String c=a;
        a=b;
        b=c;
    }

    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));

    }
}
