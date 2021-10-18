package com.gmall.gmall2021publisher.service;

import com.gmall.gmall2021publisher.mapper.ProductStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * @author 刘帅
 * @create 2021-10-09 18:25
 */

@Service
public class ProductStatsServiceImpl implements ProductStatsService{
    @Override
    public BigDecimal getGMV(int date) {
        return null;
    }

    @Autowired
    ProductStatsMapper productStatsMapper;
}
