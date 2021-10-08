package com.utils;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;


/**
 * @author 刘帅
 * @create 2021-09-25 15:33
 */


public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {

        String redisKey = "DIM:" + tableName + ":" + id;

        //查询Redis
        Jedis jedis = RedisUtil.getJedis();
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null) {

            //重置过期时间
            jedis.expire(redisKey, 24 * 3600);

            //归还连接
            jedis.close();

            //返回结果
            return JSON.parseObject(dimInfoJsonStr);
        }

        //拼接查询的SQL语句
        String querySQL = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";
        System.out.println(querySQL);
        //查询数据
        List<JSONObject> list = JdbcUtil.queryList(connection, querySQL, JSONObject.class, false);
        JSONObject jsonObject = list.get(0);

        //写入Redis
        jedis.set(redisKey, jsonObject.toJSONString());
        jedis.expire(redisKey, 24 * 3600);
        jedis.close();

        //返回结果
        return jsonObject;

    }

    public static void deleteDimInfo(String tableName, String id) {
        String redisKey = "DIM:" + tableName + ":" + id;
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        JSONObject dim_base_trademark = getDimInfo(connection, "DIM_BASE_TRADEMARK", "12");
        long second = System.currentTimeMillis();
        JSONObject dim_base_trademark1 = getDimInfo(connection, "DIM_BASE_TRADEMARK", "12");
        long end = System.currentTimeMillis();

        System.out.println(second - start);
        System.out.println(end - second);

        System.out.println(dim_base_trademark);
        System.out.println(dim_base_trademark1);

        connection.close();
    }

}