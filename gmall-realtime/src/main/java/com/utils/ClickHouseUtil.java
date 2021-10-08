package com.utils;

import com.bean.TransientSink;
import com.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.connector.jdbc.JdbcSink;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author 刘帅
 * @create 2021-09-30 10:32
 */


public class ClickHouseUtil {
    public static <T> SinkFunction<T> getClickHouseSink(String sql) {
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {


                        Class<?> aClass = t.getClass();
                        Field[] fields = aClass.getDeclaredFields();

//                        Method[] methods = aClass.getMethods();
//                        for (int i = 0; i < methods.length; i++) {
//                            Method method = methods[i];
//                            method.setAccessible(true);
//                            try {
//                                method.invoke(t);
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                        }

                        //遍历属性
                        int j = 0;
                        for (int i = 0; i < fields.length; i++) {
                            try {

                                //通过反射的方式获取值
                                Field field = fields[i];
                                field.setAccessible(true);
                                Object value = field.get(t);

                                //获取字段上的注解
                                TransientSink transientSink = field.getAnnotation(TransientSink.class);
                                if (transientSink != null) {
                                    j = j + 1;
                                    continue;
                                }

                                //给预编译SQL赋值
                                preparedStatement.setObject(i + 1 - j, value);

                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }
}
