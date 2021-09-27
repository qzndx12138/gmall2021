package com.app.dwm;

import com.alibaba.fastjson.JSON;
import com.bean.OrderDetail;
import com.bean.OrderInfo;
import com.bean.OrderWide;
import com.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * 需求：DWM层-订单宽表
 * @author 刘帅
 * @create 2021-09-25 15:39
 */


public class OrderWideApp {
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
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        DataStreamSource<String> orderInfoStrDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId));

        //3. 将数据转化为JavaBean对象
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(new MapFunction<String, OrderInfo>() {
            @Override
            public OrderInfo map(String value) throws Exception {
                OrderInfo orderInfo = JSON.parseObject(value, OrderInfo.class);

                String create_time = orderInfo.getCreate_time();    //获取创建时间

                String[] date_time = create_time.split(" ");
                orderInfo.setCreate_date(date_time[0]);
                orderInfo.setCreate_hour(date_time[1].split(":")[0]);

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

                return orderInfo;
            }
        });

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(line -> {
            OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
            String create_time = orderDetail.getCreate_time();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
            return orderDetail;
        });

        //4. 提取事件时间生成WaterMark
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWMDS = orderInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
            @Override
            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                return element.getCreate_ts();
            }
        }));
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWMDS = orderDetailDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
            @Override
            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                return element.getCreate_ts();
            }
        }));

        //5.订单与订单明细双流join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithWMDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailWithWMDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-2), Time.seconds(2))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        //6.关联维度信息
        orderWideDS.map(new RichMapFunction<OrderWide, OrderWide>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public OrderWide map(OrderWide value) throws Exception {

                Long user_id = value.getUser_id();
                Long province_id = value.getProvince_id();

                return null;
            }
        });

        //7. 将数据写入kafka

        //8. 启动任务
        env.execute();
    }
}
