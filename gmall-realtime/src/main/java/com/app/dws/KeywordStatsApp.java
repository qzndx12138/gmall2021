package com.app.dws;

import com.app.func.SplitFunction;
import com.bean.KeywordStats;
import com.utils.ClickHouseUtil;
import com.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 刘帅
 * @create 2021-10-08 15:40
 */


public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //1、获取执行环境                                                                      //生产环境,并行度设置与Kafka主题的分区数一致
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.1 指定状态后端并开放CK
//        env.setStateBackend(new FsStateBackend("hdfs://CJhadoop102:8020/flink-cdc/ck"));
//        env.enableCheckpointing(5000L);         //5s开启一次Checkpoint，指两次开始时的间隔时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);     // 超时时间
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);   //同时最多存在两个，因为有下一个参数的存在，所以这个参数永远也用不到
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);     //上一次的尾和下一次的头间隔大于2s


        //2、使用DDL方式读取kafka主题中的元素数据，并创建表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";

        tableEnv.executeSql("" +
                "CREATE TABLE page_view ( " +
                "  `common` MAP<STRING,STRING>, " +
                "  `page` MAP<STRING,STRING>, " +
                "  `ts` BIGINT, " +
                "  `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "  WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                ") WITH ( " +
                MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) +
                ")");

        //3、过滤数据  "item_type":keyword "item":"苹果手机"
        Table filterTable = tableEnv.sqlQuery("" +
                "select " +
                "    page['item'] full_word, " +
                "    rt " +
                "from page_view " +
                "where  " +
                "    page['item_type'] = 'keyword' " +
                "and " +
                "    page['item'] is not null");
        tableEnv.createTemporaryView("filter_table", filterTable);

        //4、注册UDTF函数 并且坐分词处理
        tableEnv.createTemporarySystemFunction("SplitFunc", SplitFunction.class);
        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT  " +
                "    word, " +
                "    rt " +
                "FROM filter_table,  " +
                "LATERAL TABLE(SplitFunc(full_word))");
        tableEnv.createTemporaryView("split_table", splitTable);

        //5、按照单词分组计算WordCount
        Table resultTable = tableEnv.sqlQuery(" " +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +
                "    count(*) ct, " +
                "    UNIX_TIMESTAMP() as ts " +
                "from split_table " +
                "group by word,TUMBLE(rt, INTERVAL '10' SECOND)");

        //6、将动态表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        //7、写出数据到Clickhouse
        keywordStatsDataStream.addSink(ClickHouseUtil.getClickHouseSink("insert into keyword_stats(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //8、启动任务
        env.execute();

    }
}
