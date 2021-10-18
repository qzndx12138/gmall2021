package com.gmall.gmall2021publisher.controller;

import com.gmall.gmall2021publisher.service.ProductStatsService;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @author 刘帅
 * @create 2021-10-09 18:34
 * Desc: sugar处理类
 * 主要接收用户请求，并做出相应。根据sugar不同的组件，返回不同的格式
 * Desc: 大屏展示的控制层
 *  * 主要职责：接收客户端的请求(request)，对请求进行处理，并给客户端响应(response)
 *  *
 *  * @RestController = @Controller + @ResponseBody
 *  * @RequestMapping()可以加在类和方法上 加在类上，就相当于指定了访问路径的命名空间
 */

@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    //将service注入进来
    @Autowired
    ProductStatsService productStatsService;
    /*
    {
        "status": 0,
        "msg": "",
        "data": 1201081.1632389291
    }
     */
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date) {
        if(date==0){
            date=now();
        }
        BigDecimal gmv = productStatsService.getGMV(date);
        String json = "{   \"status\": 0,  \"data\":" + gmv + "}";
        return  json;
    }

    private int now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return   Integer.valueOf(yyyyMMdd);
    }

}
