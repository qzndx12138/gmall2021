package com.utils;


import com.alibaba.fastjson.JSONObject;
import com.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;


/**
 * @author 刘帅
 * @create 2021-09-25 15:33
 */


public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {

        //拼接查询的SQL语句
        String querySQL = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";
        System.out.println(querySQL);
        //查询数据
        List<JSONObject> list = JdbcUtil.queryList(connection, querySQL, JSONObject.class, false);

        //返回结果
        return list.get(0);

    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "1"));

        connection.close();
    }

}