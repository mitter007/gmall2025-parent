package com.spdbccc.gmall.mapper;

import com.spdbccc.gmall.bean.UserChangeCtPerType;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
@Mapper
public interface UserStatsMapper {

    @Select("select 'backCt'     type,\n" +
            "       sum(back_ct) back_ct\n" +
            "from dws_user_user_login_window\n" +
            "         partition (par#{date})\n" +
            "union all\n" +
            "select 'activeUserCt' type,\n" +
            "       sum(uu_ct)     uu_ct\n" +
            "from dws_user_user_login_window\n" +
            "         partition (par#{date})\n" +
            "union all\n" +
            "select 'newUserCt'      type,\n" +
            "       sum(register_ct) register_ct\n" +
            "from dws_user_user_register_window\n" +
            "         partition (par#{date});")
    List<UserChangeCtPerType> selectUserChangeCtPerType(@Param("date")Integer date);
}
