package com.spdbccc.gmall.service;

import com.spdbccc.gmall.bean.UserChangeCtPerType;
import com.spdbccc.gmall.mapper.UserStatsMapper;
import com.spdbccc.gmall.service.impl.UserStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserStatsServiceImpl implements UserStatsService {

    @Autowired
    UserStatsMapper userStatsMapper;

    @Override
    public List<UserChangeCtPerType> getUserChangeCt(Integer date) {
        return userStatsMapper.selectUserChangeCtPerType(date);
    }
}
