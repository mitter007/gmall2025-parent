package com.test;

import com.alibaba.fastjson.JSONObject;

/**
 * ClassName: JsonTest
 * Package: com.test
 * Description:
 *
 * @Author JWT
 * @Create 2025/6/26 15:35
 * @Version 1.0
 */
public class JsonTest {
    public static void main(String[] args) {
        String s="{\"actions\":[{\"action_id\":\"favor_add\",\"item\":\"14\",\"item_type\":\"sku_id\",\"ts\":1749519486255}],\"common\":{\"ar\":\"31\",\"ba\":\"iPhone\",\"ch\":\"Appstore\",\"is_new\":\"1\",\"md\":\"iPhone 14\",\"mid\":\"mid_483\",\"os\":\"iOS 13.3.1\",\"sid\":\"1ec4030c-bf84-4c6f-840e-ed233b94f2f6\",\"uid\":\"274\",\"vc\":\"v2.1.134\"},\"displays\":[{\"item\":\"9\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":0},{\"item\":\"2\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":1},{\"item\":\"13\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":2},{\"item\":\"27\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":3},{\"item\":\"6\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":4},{\"item\":\"6\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":5},{\"item\":\"20\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":6},{\"item\":\"11\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":7}],\"err\":{\"error_code\":1608,\"msg\":\" Exception in thread \\\\  java.net.SocketTimeoutException\\\\n \\\\tat com.atgugu.gmall2020.mock.bean.AppError.main(AppError.java:xxxxxx)\"},\"page\":{\"during_time\":18207,\"from_pos_id\":8,\"from_pos_seq\":15,\"item\":\"14\",\"item_type\":\"sku_id\",\"last_page_id\":\"home\",\"page_id\":\"good_detail\"},\"ts\":1749519484255}";
        JSONObject jsonObject = JSONObject.parseObject(s);
        System.out.println(jsonObject);
        Object err = jsonObject.get("err");
//        System.out.println(err);

        jsonObject.getJSONObject("err");
        jsonObject.remove("err");
        System.out.println(jsonObject);
    }
}
