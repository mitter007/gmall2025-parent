package com.spdbccc.gmall.controller;

import com.spdbccc.gmall.bean.UserChangeCtPerType;
import com.spdbccc.gmall.service.impl.UserStatsService;
import com.spdbccc.gmall.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/gmall/realtime/user")
public class UserStatsController {

    @Autowired
    private UserStatsService userStatsService;

    @RequestMapping("/userChangeCt")
    public String getUserChange(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {

        if (date == 1) {
            date = DateFormatUtil.now();
        }

        List<UserChangeCtPerType> userChangeCtList = userStatsService.getUserChangeCt(date);

        if (userChangeCtList == null) {
            return "";
        }

        StringBuilder rows = new StringBuilder("[");

        for (int i = 0; i < userChangeCtList.size(); i++) {

            UserChangeCtPerType userChangeCt = userChangeCtList.get(i);
            String type = userChangeCt.getType();
            Integer userCt = userChangeCt.getUserCt();

            rows.append("{\n" +
                    "\t\"type\": \"" + type + "\",\n" +
                    "\t\"user_ct\": \"" + userCt + "\"\n" +
                    "}");

            if (i < userChangeCtList.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"columns\": [\n" +
                "      {\n" +
                "        \"name\": \"变动类型\",\n" +
                "        \"id\": \"type\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"用户数\",\n" +
                "        \"id\": \"user_ct\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }
}
