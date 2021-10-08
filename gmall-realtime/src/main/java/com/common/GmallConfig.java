package com.common;

/**
 * @author 刘帅
 * @create 2021-09-23 18:59
 */


public class GmallConfig {
    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:CJhadoop102,CJhadoop103,CJhadoop104:2181";

    public static final String CLICKHOUSE_URL="jdbc:clickhouse://CJhadoop102:8123/default";

    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

}
