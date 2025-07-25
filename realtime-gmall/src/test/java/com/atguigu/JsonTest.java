package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * ClassName: JsonTest
 * Package: com.atguigu
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/25 8:41
 * @Version 1.0
 */
public class JsonTest {
    public static void main(String[] args) {
        String s="{\"common\":{\"ar\":\"28\",\"uid\":\"88\",\"os\":\"Android 13.0\",\"ch\":\"oppo\",\"is_new\":\"1\",\"md\":\"Redmi k50\",\"mid\":\"mid_361\",\"vc\":\"v2.1.134\",\"ba\":\"Redmi\",\"sid\":\"3cd0e6b5-e81a-4468-ab94-6e6315853590\"},\"page\":{\"page_id\":\"cart\",\"during_time\":10544},\"ts\":1654676304427}";
        JSONObject jsonObject = JSON.parseObject(s);
        String uid = jsonObject.getJSONObject("common").getString("uid");
        String pageId = jsonObject.getJSONObject("page").getString("last_page_id");
        System.out.println(uid);
        System.out.println(pageId);

        boolean flag = jsonObject.getJSONObject("common").getString("uid") != null && (jsonObject.getJSONObject("page").getString("last_page_id")==null || jsonObject.getJSONObject("page").getString("last_page_id").equals("login"));
        System.out.println(flag);

    }
}
