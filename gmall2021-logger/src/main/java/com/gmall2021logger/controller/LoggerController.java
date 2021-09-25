package com.gmall2021logger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author 刘帅
 * @create 2021-09-18 16:24
 */

//@Controller
@RestController     //@RestController = @Controller + @ResponseBody
@Slf4j      //创建一个log对象
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;
//    @ResponseBody   //表示返回该返回的内容，而不去寻找其所对应的对象
    @RequestMapping("test1")
    public String test1(){
        System.out.println("111111111111111");
        return "success";
    }
    @RequestMapping("test2")
    public String test2(@RequestParam("name") String name,
                        @RequestParam("age") int age) {
        System.out.println(name + ":" + age);
        return "success";
    }

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr){
//        //打印
//        System.out.println("aaaaaaaaaaaaaaaaa");

        //落盘
        log.info(jsonStr);

        //写入kafka
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }

}
