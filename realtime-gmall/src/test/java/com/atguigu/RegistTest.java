package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * ClassName: RegistTest
 * Package: com.atguigu
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/25 10:30
 * @Version 1.0
 */
public class RegistTest {
    public static void main(String[] args) {
        String s = "{\"create_time\":\"2025-07-01 18:53:22\",\"id\":2291,\"type\":\"insert\"}";
        JSONObject jsonObject = JSON.parseObject(s);
        String type = jsonObject.getString("type");
        System.out.println(type);





    }
}
