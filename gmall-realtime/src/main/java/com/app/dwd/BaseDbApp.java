package com.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.app.func.DimSinkFunction;
import com.app.func.MyStringDebeziumDeserializationSchema;
import com.app.func.TableProcessFunction;
import com.bean.TableProcess;
import com.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 *  需求：准备业务数据DWD层：1、接收Kafka数据，过滤空值数据 2、实现动态分流功能 3、把分好的流保存到对应表、主题中（kafka、hbase）
 * @author 刘帅
 * @create 2021-09-23 19:04
 */


public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境(在实际开发，分区数要设置为kafka的副本数)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //1.1 指定状态后端并开放CK
//        env.setStateBackend(new FsStateBackend("hdfs://CJhadoop102:8020/flink-cdc/ck"));
//        env.enableCheckpointing(5000L);         //5s开启一次Checkpoint，指两次开始时的间隔时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);     // 超时时间
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);   //同时最多存在两个，因为有下一个参数的存在，所以这个参数永远也用不到
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);     //上一次的尾和下一次的头间隔大于2s

        //TODO 2.读取 Kafka ods_base_db 主题创建   主流
        String topic = "ods_base_db";
        String groupId = "base_db_app";
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 3.将主流每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStreamSource.map(JSON::parseObject);

        //TODO 4.过滤空值数据  删除数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String type = value.getString("type");
                return !"delete".equals(type);
            }
        });

        //TODO 5.使用 FlinkCDC 读取配置信息表
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("CJhadoop102")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall_realtime")
                .tableList("gmall_realtime.table_process")
                .startupOptions(StartupOptions.initial())        //initial 第一次打印所有的数据，然后之后只打印增量及变化，earliest就只打印执行完程序之后新增及变化的
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);
        mysqlDS.print();
        //TODO 6.将配置信息表的流转换为             广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlDS.broadcast(mapStateDescriptor);

        //TODO 7.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = filterDS.connect(broadcastStream);

        //TODO 8.处理连接后的流
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("hbaseTag") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaDS = broadcastConnectedStream.process(new TableProcessFunction(mapStateDescriptor, outputTag));

        //TODO 9.将kafka流数据以及HBase流数据分别写入Kafka和Phoenix
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(outputTag);
        kafkaDS.print("Kafka>>>>>>>>>");
        hbaseDS.print("HBase>>>>>>>>>");

        //使用自定义Sink写出数据到Phoenix
        hbaseDS.addSink(new DimSinkFunction());

        //将数据写入Kafka
        kafkaDS.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            //element : {"database":"gmall-210426-flink","tableName":"base_trademark","after":""....}
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(element.getString("sinkTable"),
                        element.getString("after").getBytes());
            }
        }));
        //10. 启动任务
        env.execute();
    }
}
