package com.spdbccc.gmall.service.impl;

import com.spdbccc.gmall.bean.UserChangeCtPerType;
import com.spdbccc.gmall.bean.UserPageCt;
import com.spdbccc.gmall.bean.UserTradeCt;

import java.util.List;

public interface UserStatsService {
    List<UserChangeCtPerType> getUserChangeCt(Integer date);
    List<UserPageCt> getUvByPage(Integer date);

    List<UserTradeCt> getTradeUserCt(Integer date);
}
