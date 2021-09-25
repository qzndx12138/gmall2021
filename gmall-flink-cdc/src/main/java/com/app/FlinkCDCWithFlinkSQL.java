package com.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author 刘帅
 * @create 2021-09-22 19:11
 */


public class FlinkCDCWithFlinkSQL {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2. 使用DDL方式创建表
        tableEnv.executeSql("create table base_trademark( " +
                "     id int, " +
                "     tm_name string, " +
                "     logo_url string " +
                ") with ( " +
                "     'connector' = 'mysql-cdc', " +
                "     'hostname' = 'CJhadoop102', " +
                "     'port' = '3306', " +
                "     'username' = 'root', " +
                "     'password' = 'root', " +
                "     'database-name' = 'gmall-flink', " +
                "     'table-name' = 'base_trademark')");

        //3. 执行查询
        Table table = tableEnv.sqlQuery("select * from base_trademark");

        //4. 打印数据
        tableEnv.toRetractStream(table, Row.class).print();          //撤回流

        //5. 开启任务
        env.execute();
    }
}
