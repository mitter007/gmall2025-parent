package com.spdbccc.gmall.service.impl;

import com.spdbccc.gmall.bean.UserChangeCtPerType;

import java.util.List;

public interface UserStatsService {
    List<UserChangeCtPerType> getUserChangeCt(Integer date);
}
