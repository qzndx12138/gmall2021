package com.gmall.gmall2021publisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

/**
 * @author 刘帅
 * @create 2021-10-09 18:13
 * Desc: 商品统计Mapper
 */


public interface ProductStatsMapper {
    //获取商品交易额
    @Select("select sum(order_amount) order_amount  " +
            "from product_stats_2021 where toYYYYMMDD(stt)=#{date}")
    public BigDecimal getGMV(int date);
}
