package com.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.app.func.MyStringDebeziumDeserializationSchema;
import com.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 需求：ods层业务数据库数据采集
 * @author 刘帅
 * @create 2021-09-22 20:22
 */


public class FlinkCDCApp {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //1.1 指定状态后端并开放CK
//        env.setStateBackend(new FsStateBackend("hdfs://CJhadoop102:8020/flink-cdc/ck"));
//        env.enableCheckpointing(5000L);         //5s开启一次Checkpoint，指两次开始时的间隔时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);     // 超时时间
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);   //同时最多存在两个，因为有下一个参数的存在，所以这个参数永远也用不到
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);     //上一次的尾和下一次的头间隔大于2s

        //2. 创建MYSQL CDC Source 并创建流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("CJhadoop102")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall-flink")            //库名
//                .tableList("gmall-flink.base_trademark")        //表名，必须带上库名
                .startupOptions(StartupOptions.latest())   //initial 第一次打印所有的数据，然后之后只打印增量及变化，earliest就只打印执行完程序之后新增及变化的
                .deserializer(new MyStringDebeziumDeserializationSchema())//使用自定义的反序列化类（自定义解析器）
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //3. 打印数据并将数据写入kafka
        dataStreamSource.print();
        dataStreamSource.addSink(MyKafkaUtil.getKafkaProducer("ods_base_db"));

        //4. 启动任务
        env.execute();
    }
}
