package com.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 *  需求：准备用户行为日志DWD层：1、识别新老用户 2、利用侧输出流实现数据拆分 3、将不同流的数据推送下游的Kafka的不同Topic中
 * @author 刘帅
 * @create 2021-09-23 11:06
 */


//数据流:web/app -> nginx -> SpringBoot -> Kafka(ODS) -> FlinkApp   -> Kafka(DWD)
//程  序:mock    -> nginx -> Logger     -> Kafka(ZK)  -> BaseLogApp -> Kafka(ZK)
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //1.1 指定状态后端并开放CK
//        env.setStateBackend(new FsStateBackend("hdfs://CJhadoop102:8020/flink-cdc/ck"));
//        env.enableCheckpointing(5000L);         //5s开启一次Checkpoint，指两次开始时的间隔时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);     // 超时时间
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);   //同时最多存在两个，因为有下一个参数的存在，所以这个参数永远也用不到
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);     //上一次的尾和下一次的头间隔大于2s
        //2. 读取kafka ods_base_log 主题的数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer("ods_base_log", "base_log_app"));

        //3. 将每行的数据转化为JSON格式
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("Dirty"){};   //创建一个“Dirty”侧输出流，用于存放脏数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);    //将传入的字符串s转化为JSON格式
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyOutputTag, s);      //如果字符串s不能转化为json，即为脏数据，将其放入Dirty侧输出流
                }
            }
        });

        jsonObjDS.getSideOutput(dirtyOutputTag).print("Dirty");     //获取dirty侧输出流的数据，并打印输出,定义的侧输出流不能直接输出

        //4. 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        //5. 使用状态编程实现新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> valueState;

            @Override           //open方法最先被执行
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //a.获取 is_new 标记
                String isNew = value.getJSONObject("common").getString("is_new");
                //b.如果标记为"1",处理
                if ("1".equals(isNew)) {
                    //获取状态数据
                    String state = valueState.value();

                    if (state != null) {     //如果状态不为空，就把is_new改为0
                        value.getJSONObject("common").put("is_new", "0");
                    } else {     //如果状态为空，就把状态设置不为空（设置为什么都可以）
                        valueState.update("0");
                    }
                }

                return value;
            }
        });

        //6. 使用测输出流实现分流   页面  启动  曝光
        OutputTag<String> startOutputTag = new OutputTag<String>("start"){};       //创建start侧输出流
        OutputTag<String> displayOutputTag = new OutputTag<String>("display"){};    //创建display侧输出流
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                //获取启动数据
                String start = jsonObject.getString("start");

                if (start != null) {      //如果start不为空，则将其写入start侧输出流
                    context.output(startOutputTag, start);
                } else {
                    collector.collect(jsonObject.toJSONString());   //如果start为空，数据全部放入主流，稍后在提取出来，具体分类

                    JSONArray displays = jsonObject.getJSONArray("displays");   //提取出曝光日志

                    if (displays != null && displays.size() > 0) {    //如果曝光数据不为空，则提取出每条曝光数据的pageID写入JSON，然后再遍历数组，将曝光数据写入侧输出流
                        String pageID = jsonObject.getJSONObject("page").getString("page_id");

                        //遍历数组
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displaysJSONObject = displays.getJSONObject(i);

                            displaysJSONObject.put("page_id", pageID );

                            context.output(displayOutputTag, displaysJSONObject.toJSONString());
                        }
                    }
                }
            }
        });

        //7. 将多个流分别写入kafka对应的topic主题
        DataStream<String> startDS = pageDS.getSideOutput(startOutputTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayOutputTag);

        pageDS.print("页面数据>>>>");
        startDS.print("启动数据>>>>");
        displayDS.print("曝光数据>>>>");

        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        //8. 启动任务
        env.execute();
    }
}
