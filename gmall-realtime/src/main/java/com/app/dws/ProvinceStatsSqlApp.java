package com.app.dws;

import com.bean.ProvinceStats;
import com.utils.ClickHouseUtil;
import com.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 刘帅
 * @create 2021-10-08 11:32
 */


public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.1 指定状态后端并开放CK
//        env.setStateBackend(new FsStateBackend("hdfs://CJhadoop102:8020/flink-cdc/ck"));
//        env.enableCheckpointing(5000L);         //5s开启一次Checkpoint，指两次开始时的间隔时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);     // 超时时间
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);   //同时最多存在两个，因为有下一个参数的存在，所以这个参数永远也用不到
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);     //上一次的尾和下一次的头间隔大于2s

        //2、使用DDL方式创建表读取kafka数据  dwd_order_wide
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("CREATE TABLE order_wide (\n" +
                "  `province_id` BIGINT,\n" +
                "  `province_name` STRING,\n" +
                "  `province_area_code` STRING,\n" +
                "  `province_iso_code` STRING,\n" +
                "  `province_3166_2_code` STRING,\n" +
                "  `order_id` BIGINT,\n" +
                "  `split_total_amount` DECIMAL,\n" +
                "  `create_time` STRING,\n" +
                "  `rt` as TO_TIMESTAMP(create_time),\n" +
                "  WATERMARK FOR rt AS rt - INTERVAL '2' SECOND\n" +
                ") WITH (\n" +
                        MyKafkaUtil.getKafkaDDL(orderWideTopic,groupId) +
                ")");

        //3、执行查询
        Table sqlQuery = tableEnv.sqlQuery("select " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    province_id, " +
                "    province_name, " +
                "    province_area_code, " +
                "    province_iso_code, " +
                "    province_3166_2_code, " +
                "    sum(split_total_amount) order_amount, " +
                "    count(distinct order_id) order_count, " +
                "    UNIX_TIMESTAMP() as ts " +
                "from order_wide " +
                "group by " +
                "    province_id, " +
                "    province_name, " +
                "    province_area_code, " +
                "    province_iso_code, " +
                "    province_3166_2_code, " +
                "    TUMBLE(rt, INTERVAL '10' SECOND)");
        //4、将查询结果转化为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(sqlQuery, ProvinceStats.class);

        //5、将数据写入clickhouse
        provinceStatsDataStream.addSink(ClickHouseUtil.getClickHouseSink("insert into province_stats_210426 values(?,?,?,?,?,?,?,?,?,?)"));

        //6、启动任务
        env.execute();
    }
}
