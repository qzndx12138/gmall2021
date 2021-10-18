package com.gmall.gmall2021publisher.service;

import java.math.BigDecimal;

/**
 * @author 刘帅
 * @create 2021-10-09 18:20
 * Desc: 商品统计接口
 */

public interface ProductStatsService {
    //获取某一天的总交易额
    public BigDecimal getGMV(int date);
}
