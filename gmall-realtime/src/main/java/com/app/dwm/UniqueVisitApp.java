package com.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * 需求：DWM层-访客UV计算
 * @author 刘帅
 * @create 2021-09-24 16:11
 */

//log日志数据
//数据流: web/app -> nginx -> SpringBoot -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWM)
//程  序: Mock   -> Nginx -> Logger -> Kafka(ZK) -> BaseLogApp -> Kafka -> UniqueVisitApp -> Kafka
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {

        //1. 获取执行环境     在生产环境中分区数设置为kafka topic分区数一致
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //1.1 指定状态后端并开放CK
//        env.setStateBackend(new FsStateBackend("hdfs://CJhadoop102:8020/flink-cdc/ck"));
//        env.enableCheckpointing(5000L);         //5s开启一次Checkpoint，指两次开始时的间隔时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);     // 超时时间
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);   //同时最多存在两个，因为有下一个参数的存在，所以这个参数永远也用不到
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);     //上一次的尾和下一次的头间隔大于2s

        //2. 读取kafka dwd_page_log 主题数据创建流
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //3. 将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        //两者效果等同
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
//            @Override
//            public JSONObject map(String value) throws Exception {
//                return JSON.parseObject(value);
//            }
//        });

        //4. 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(line -> line.getJSONObject("common").getString("mid"));
        //两者效果等同
//        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
//            @Override
//            public String getKey(JSONObject value) throws Exception {
//                return value.getJSONObject("common").getString("mid");
//            }
//        });

        //5. 使用状态编程进行数据去重（过滤）       由于涉及到状态编程，所以使用Rich富函数
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> valueState;  //声明状态
            private SimpleDateFormat sdf;   //日期转化类对象

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();       //设置状态信息保存一天，并且如果遇到相同的数据之后，保留时间一天将会被重置

                valueStateDescriptor.enableTimeToLive(ttlConfig);

                valueState = getRuntimeContext().getState(valueStateDescriptor);

                sdf = new SimpleDateFormat("yyyy-MM-dd");

            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                //1. 获取上一跳页面ID
                String lastPage = value.getJSONObject("page").getString("last_page_id");

                //2. 判断上一跳页面ID是否为null,如果为null，则保留，如果不为null，则获取状态，进行下一步判断
                if (lastPage != null) {

                    //获取状态
                    String stateData = valueState.value();

                    //将状态数据转化为时间格式
                    String newData = sdf.format(value.getLong("ts"));

                    //如果状态为空，或者状态中的数据与当前数据的日期不同，则保留，且更新状态数据
                    if (stateData == null || !stateData.equals(newData)) {

                        //更新状态
                        valueState.update(newData);

                        return true;

                    } else {
                        return false;
                    }

                } else {

                    return false;
                }

            }
        });

        //6. 将数据写入kafka
        filterDS.print("filterDS>>>>>");
        filterDS.map(JSONAware::toJSONString)  //把JSONObject格式转化为JSON字符串，然后再写入kafka
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //7. 启动任务
        env.execute();
    }
}
