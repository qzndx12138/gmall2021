package com.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.app.func.AsyncDimFunction;
import com.bean.OrderDetail;
import com.bean.OrderInfo;
import com.bean.OrderWide;
import com.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 需求：DWM层-订单宽表
 * @author 刘帅
 * @create 2021-09-25 15:39
 */

//数据流：web/app -> Nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)/Phoenix(DIM) -> FlinkApp -> Kafka(DWM)
//程  序：Mock -> Mysql -> FlinkCDCApp -> Kafka -> BaseDbApp -> Kafka/Phoenix -> OrderWideApp(Redis) -> Kafka
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
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWMDS = orderInfoDS
                //  提取时间事件，生成WaterMark             提取事件时间，设置为乱序，乱序程度设置为2s      获取一个时间戳
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //获取一个时间戳
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
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
                //使用intervalJoin的方式（最常用），以id进行join
                .intervalJoin(orderDetailWithWMDS.keyBy(OrderDetail::getOrder_id))
                //设置上下线
                .between(Time.seconds(-2), Time.seconds(2))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        //6.关联维度信息
//        orderWideDS.map(new RichMapFunction<OrderWide, OrderWide>() {
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//            }
//
//            @Override
//            public OrderWide map(OrderWide value) throws Exception {
//
//                Long user_id = value.getUser_id();
//                Long province_id = value.getProvince_id();
//
//                return null;
//            }
//        });

        //6.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserInfoDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                new AsyncDimFunction<OrderWide>("DIM_USER_INFO") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        String gender = dimInfo.getString("GENDER");
                        orderWide.setUser_gender(gender);

                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        long ts = System.currentTimeMillis() - sdf.parse(birthday).getTime();

                        long age = ts / (1000L * 60 * 60 * 24 * 365);

                        orderWide.setUser_age((int) age);
                    }
                },
                60,
                TimeUnit.SECONDS);

//        orderWideWithUserInfoDS.print("User>>>>>>>>>>>>>>");

        //6.2 关联省份维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserInfoDS,
                new AsyncDimFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject jsonObject) throws ParseException {
                        input.setProvince_name(jsonObject.getString("NAME"));
                        input.setProvince_area_code(jsonObject.getString("AREA_CODE"));
                        input.setProvince_iso_code(jsonObject.getString("ISO_CODE"));
                        input.setProvince_3166_2_code(jsonObject.getString("ISO_3166_2"));
                    }
                }, 60, TimeUnit.SECONDS);

        //6.3 关联SKU维度 (SKU维度一定要在SPU维度、品类维度和品牌维度之前)
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS,
                new AsyncDimFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject jsonObject) throws ParseException {
                        input.setSku_name(jsonObject.getString("SKU_NAME"));
                        input.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        //6.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS,
                new AsyncDimFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject jsonObject) throws ParseException {
                        input.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //6.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(orderWideWithSpuDS,
                new AsyncDimFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getTm_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject jsonObject) throws ParseException {
                        input.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //6.6 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideCategory3DS = AsyncDataStream.unorderedWait(orderWideWithTmDS,
                new AsyncDimFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getCategory3_name();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject jsonObject) throws ParseException {
                        input.setCategory3_name(jsonObject.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //7. 将数据写入kafka
        orderWideCategory3DS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));

        //8. 启动任务
        env.execute();
    }
}
