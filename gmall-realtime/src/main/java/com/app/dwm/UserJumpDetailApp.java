package com.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 需求：DWM层-跳出明细计算
 * @author 刘帅
 * @create 2021-09-25 10:26
 */

//数据流：web/app -> Nginx -> SpringBoot -> Kafka(ODS) -> FLinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWM)

//程  序： Mock   -> Nginx -> Logger     -> Kafka(ZK)  -> BaseLogApp -> Kafka -> UserJumpDetailApp -> Kafka
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境     在生产环境中分区数设置为kafka topic分区数一致
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //1.1 指定状态后端并开放CK
//        env.setStateBackend(new FsStateBackend("hdfs://CJhadoop102:8020/flink-cdc/ck"));
//        env.enableCheckpointing(5000L);         //5s开启一次Checkpoint，指两次开始时的间隔时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);     // 超时时间
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);   //同时最多存在两个，因为有下一个参数的存在，所以这个参数永远也用不到
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);     //上一次的尾和下一次的头间隔大于2s

        //2. 读取kafka dwd_pag_log 主题中的数据来创建流
        String groupId = "dwd_page_log";
        String sourceTopic = "user_jump_detail_app";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //3. 将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjectSingleOutputStreamOperator = kafkaDS.map(JSON::parseObject)
                //  提取时间事件，生成WaterMark                                     提取事件时间，设置为乱序，乱序程度设置为2s      获取一个时间戳
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        //4. 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectSingleOutputStreamOperator.keyBy(line -> line.getJSONObject("common").getString("mid"));

        //5. 定义模式序列                                             必须以.begin()方法 开头   where为判断条件
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("begin").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {   //.next（）严格近邻，过滤条件与第一条一致
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10)); //within加上窗口长度，超过窗口长度的数据会被丢弃

        //6. 将模式作用在流上
        PatternStream<JSONObject> patternStream  = CEP.pattern(keyedStream, pattern);

        //7. 提取匹配上的数据以及超时数据
        OutputTag<String> timeOutputTag = new OutputTag<String>("TimeOut") {};

        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutputTag, new PatternTimeoutFunction<JSONObject, String>() {
            @Override       //取出集合中的第一个事件输出
            public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("begin").get(0).toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("begin").get(0).toJSONString();
            }
        });

        //8. 拼接两个流并写入Kafka
        DataStream<String> timeOutputDS = selectDS.getSideOutput(timeOutputTag);    //将超时事件写入侧输出流
        DataStream<String> unionDS = selectDS.union(timeOutputDS);          //拼接

        unionDS.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        ///9. 启动任务
        env.execute();
    }
}
