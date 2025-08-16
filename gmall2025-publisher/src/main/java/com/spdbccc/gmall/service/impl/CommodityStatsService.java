package com.spdbccc.gmall.service.impl;

import com.spdbccc.gmall.bean.CategoryCommodityStats;
import com.spdbccc.gmall.bean.SpuCommodityStats;
import com.spdbccc.gmall.bean.TrademarkCommodityStats;
import com.spdbccc.gmall.bean.TrademarkOrderAmountPieGraph;

import java.util.List;

public interface CommodityStatsService {
List<TrademarkCommodityStats> getTrademarkCommodityStatsService(Integer date);

    List<TrademarkOrderAmountPieGraph> getTmOrderAmtPieGra(Integer date);

    List<CategoryCommodityStats> getCategoryStatsService(Integer date);
    List<SpuCommodityStats> getSpuCommodityStats(Integer date);
}
