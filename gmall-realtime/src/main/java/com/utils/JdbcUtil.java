package com.utils;


import com.alibaba.fastjson.JSONObject;
import com.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


/**
 * @author 刘帅
 * @create 2021-09-25 15:33
 */


public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws Exception {

        //1.创建集合用于存放结果数据
        ArrayList<T> resultList = new ArrayList<>();

        //2.预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        //3.执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        //4.遍历resultSet,给每行数据封装泛型对象并将其加入结果集中
        while (resultSet.next()) {

            //创建泛型对象
            T t = clz.newInstance();

            //给泛型对象赋值
            for (int i = 1; i < columnCount + 1; i++) {

                String columnName = metaData.getColumnName(i);
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                BeanUtils.setProperty(t, columnName, resultSet.getObject(i));

            }

            //将对象加入集合
            resultList.add(t);

        }

        preparedStatement.close();
        resultSet.close();

        //返回查询结果
        return resultList;

    }

    public static void main(String[] args) throws Exception {

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        List<JSONObject> list = queryList(connection, "select * from GMALL210325_REALTIME.DIM_BASE_TRADEMARK", JSONObject.class, true);
        long end = System.currentTimeMillis();
        List<JSONObject> list1 = queryList(connection, "select * from GMALL210325_REALTIME.DIM_BASE_TRADEMARK", JSONObject.class, true);
        long end2 = System.currentTimeMillis();

        System.out.println(end - start);  //195 211 188
        System.out.println(end2 - end);   //13  15
        System.out.println(list);
        System.out.println(list1);

    }

}
