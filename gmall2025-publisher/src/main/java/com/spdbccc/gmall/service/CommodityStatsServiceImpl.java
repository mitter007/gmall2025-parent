package com.spdbccc.gmall.service;

import com.spdbccc.gmall.bean.CategoryCommodityStats;
import com.spdbccc.gmall.bean.SpuCommodityStats;
import com.spdbccc.gmall.bean.TrademarkCommodityStats;
import com.spdbccc.gmall.bean.TrademarkOrderAmountPieGraph;
import com.spdbccc.gmall.mapper.CommodityStatsMapper;
import com.spdbccc.gmall.service.impl.CommodityStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CommodityStatsServiceImpl implements CommodityStatsService {

    @Autowired
    private CommodityStatsMapper commodityStatsMapper;

    @Override
    public List<TrademarkCommodityStats> getTrademarkCommodityStatsService(Integer date) {
        return commodityStatsMapper.selectTrademarkStats(date);
}

    @Override
    public List<TrademarkOrderAmountPieGraph> getTmOrderAmtPieGra(Integer date) {
        return commodityStatsMapper.selectTmOrderAmtPieGra(date);
    }

    @Override
    public List<CategoryCommodityStats> getCategoryStatsService(Integer date) {
        return commodityStatsMapper.selectCategoryStats(date);
    }
    @Override
    public List<SpuCommodityStats> getSpuCommodityStats(Integer date) {
        return commodityStatsMapper.selectSpuStats(date);
    }

}
