package com.app.dwm;

import com.alibaba.fastjson.JSON;
import com.bean.OrderWide;
import com.bean.PaymentInfo;
import com.bean.PaymentWide;
import com.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
/**
 * @author 刘帅
 * @create 2021-09-28 8:41
 */


/**
 * Mock -> Mysql(binLog) -> MaxWell -> Kafka(ods_base_db_m) -> BaseDBApp(修改配置,Phoenix)
 * -> Kafka(dwd_order_info,dwd_order_detail) -> OrderWideApp(关联维度,Redis) -> Kafka(dwm_order_wide)
 * -> PaymentWideApp -> Kafka(dwm_payment_wide)
 */
public class PaymentWideApp {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 指定状态后端并开放CK
//        env.setStateBackend(new FsStateBackend("hdfs://CJhadoop102:8020/flink-cdc/ck"));
//        env.enableCheckpointing(5000L);         //5s开启一次Checkpoint，指两次开始时的间隔时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);     // 超时时间
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);   //同时最多存在两个，因为有下一个参数的存在，所以这个参数永远也用不到
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);     //上一次的尾和下一次的头间隔大于2s

        //2.读取Kafka主题数据  dwd_payment_info  dwm_order_wide
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        DataStreamSource<String> paymentKafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideKafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));

        //3.将数据转换为JavaBean并提取时间戳生成WaterMark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentKafkaDS
                .map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误！！");
                                }
                            }
                        }));
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideKafkaDS
                .map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class))
                //  提取时间事件，生成WaterMark          提取事件时间，                 设置为顺序      获取一个时间戳
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        //获取一个时间戳
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override   //返回一个时间日期格式
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误！！");
                                }
                            }
                        }));

        //4.按照OrderID分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedStream = paymentInfoDS.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideKeyedStream = orderWideDS.keyBy(OrderWide::getOrder_id);

        //5.双流JOIN
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedStream.intervalJoin(orderWideKeyedStream)
                .between(Time.minutes(-15), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //打印测试
        paymentWideDS.print(">>>>>>>>>>");

        //6.将数据写入Kafka  dwm_payment_wide
        paymentWideDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));

        //7.启动任务
        env.execute();

    }

}

