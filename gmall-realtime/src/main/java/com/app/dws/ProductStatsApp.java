package com.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.app.func.AsyncDimFunction;
import com.bean.OrderWide;
import com.bean.PaymentWide;
import com.bean.ProductStats;
import com.common.GmallConstant;
import com.utils.ClickHouseUtil;
import com.utils.DateTimeUtil;
import com.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;


/**
 * @author 刘帅
 * @create 2021-09-30 16:17
 */
/**
 * 数据流：
 * 行为数据 web/app -> Nginx -> SpringBoot -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
 * 业务数据 web/app -> Nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ODS) -> FlinkApp -> Kafka/HBase(DWD/DIM) -> FlinkApp(ow,pw)
 * <p>
 * 合并 -> FlinkApp -> ClickHouse
 */



/**
 * 程序：
 * 行为数据 Mock -> Nginx -> Logger -> Kafka(ZK) -> BaseLogApp -> Kafka
 * 业务数据 Mock -> Mysql -> FlinkCDC -> Kafka(ODS) -> BaseDbApp -> Kafka/HBase(DWD/DIM) -> OrderWideApp/PaymentWideApp(ow,pw  注意：Redis)
 * <p>
 * 合并 -> ProductStatsApp -> ClickHouse
 */

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境  在生产环境中分区数设置为kafka topic分区数一致
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //1.1 指定状态后端并开放CK
//        env.setStateBackend(new FsStateBackend("hdfs://CJhadoop102:8020/flink-cdc/ck"));
//        env.enableCheckpointing(5000L);         //5s开启一次Checkpoint，指两次开始时的间隔时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);     // 超时时间
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);   //同时最多存在两个，因为有下一个参数的存在，所以这个参数永远也用不到
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);     //上一次的尾和下一次的头间隔大于2s

        //TODO 2.读取Kafka  7个主题的数据创建流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        DataStreamSource<String> pageViewDStream = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorInfoDStream = env.addSource(MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideDStream = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentWideDStream = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId));
        DataStreamSource<String> refundInfoDStream = env.addSource(MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId));
        DataStreamSource<String> cartInfoDStream = env.addSource(MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId));
        DataStreamSource<String> commentInfoDStream = env.addSource(MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId));


        //TODO 3.统一数据格式
        //3.1 pageViewDStream   点击数  曝光数
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pageViewDStream.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                Long ts = jsonObject.getLong("ts");

                //获取当前页面ID以及item_type
                JSONObject page = jsonObject.getJSONObject("page");
                if ("good_detail".equals(page.getString("page_id")) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                //获取曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        //3.2 favorInfoDStream   收藏数
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorInfoDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //3.3  orderWideDStream  订单数(去重)，件数，订单总金额
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDStream.map(line -> {

            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .orderIdSet(orderIds)
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getSplit_total_amount())
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });
        //3.4 paymentWideDStream 支付的订单数，支付的总金额
        SingleOutputStreamOperator<ProductStats> productStatsWithPayDS = paymentWideDStream.map(line -> {

            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getSplit_total_amount())
                    .paidOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });
        //3.5 refundInfoDStream 退单数，退单金额
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundInfoDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refundOrderIdSet(orderIds)
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });
        //3.6 cartInfoDStream  加购次数
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //3.7 commentInfoDStream 评价数，好评数
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentInfoDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            long goodCt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(jsonObject.getString("appraise"))) {
                goodCt = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });
        //TODO 4.Union 7个流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(productStatsWithFavorDS,
                productStatsWithOrderDS,
                productStatsWithPayDS,
                productStatsWithRefundDS,
                productStatsWithCartDS,
                productStatsWithCommentDS);

        //TODO 5.提取事件时间生成WaterMark
//        unionDS.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ProductStats>() {
//            @Override
//            public long extractAscendingTimestamp(ProductStats element) {
//                return 0;
//            }
//        });
//        unionDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ProductStats>() {
//            @Override
//            public long extractTimestamp(ProductStats element) {
//                return 0;
//            }
//        });

        SingleOutputStreamOperator<ProductStats> productStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
            @Override
            public long extractTimestamp(ProductStats element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 6.分组、开窗&聚合
        KeyedStream<ProductStats, Long> keyedStream = productStatsWithWmDS.keyBy(ProductStats::getSku_id);
        WindowedStream<ProductStats, Long, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<ProductStats> reduceDS = windowedStream.reduce(new ReduceFunction<ProductStats>() {
            @Override
            public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {

                stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                //stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                //stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                //stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                return stats1;

            }
        }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
            @Override
            public void apply(Long key, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {

                //获取数据
                ProductStats productStats = input.iterator().next();

                //补充订单数
                productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                //补充窗口的开始和结束时间
                productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                //输出数据
                out.collect(productStats);
            }
        });
        //TODO 7.关联维度信息
        //7.1 关联SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS,
                new AsyncDimFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfo.getLong("TM_ID"));
                        productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                    }
                }, 60, TimeUnit.SECONDS);
        //7.2 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new AsyncDimFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        //7.3 关联TM维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new AsyncDimFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        //7.4 关联Category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithTmDS,
                        new AsyncDimFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        //TODO 8.将数据写入ClickHouse
        productStatsWithCategory3DS.print();
        productStatsWithCategory3DS.addSink(ClickHouseUtil.getClickHouseSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute();
    }
}
