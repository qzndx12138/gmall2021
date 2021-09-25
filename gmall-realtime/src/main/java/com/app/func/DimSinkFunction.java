package com.app.func;

import com.alibaba.fastjson.JSONObject;
import com.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.lf5.util.StreamUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 *  将数据写入phoenix
 * @author 刘帅
 * @create 2021-09-24 12:00
 */


public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    //声明Phoenix 连接
    private Connection connection;

    @Override       //创建连接
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override     // value:{"database":"gmall-210426-flink","tableName":"base_trademark","after":""....}
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;
        try {
            //获取sql语句   upsert into db.tn(id,name,sex) values(...,...,...)
            String upsertSQL = getUpsertSQL(value.getString("sinkTable"), value.getJSONObject("after"));

            //打印SQL语句
            System.out.println(upsertSQL);

            //预编译sql
            preparedStatement = connection.prepareStatement(upsertSQL);

            //执行写入数据操作
            preparedStatement.execute();

            connection.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (preparedStatement != null){
                preparedStatement.close();
            }
        }
    }

    // upsert into db.tn(id,name,sex) values('123','456','789')
    private String getUpsertSQL(String tableName, JSONObject after) {

        Set<String> columns = after.keySet();   //得到after中的所有的key
        Collection<Object> values = after.values(); //得到after中所有的value

        return "upsert into" + GmallConfig.HBASE_SCHEMA + "." + tableName + "(" +
                StringUtils.join(columns,",") + ") value('" +
                StringUtils.join(values,"','") + "')";
    }
}
