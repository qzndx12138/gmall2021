package com.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bean.VisitorStats;
import com.utils.ClickHouseUtil;
import com.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author 刘帅
 * @create 2021-09-28 21:13
 */


public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境     在生产环境中分区数设置为kafka topic分区数一致
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //1.1 指定状态后端并开放CK
//        env.setStateBackend(new FsStateBackend("hdfs://CJhadoop102:8020/flink-cdc/ck"));
//        env.enableCheckpointing(5000L);         //5s开启一次Checkpoint，指两次开始时的间隔时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);     // 超时时间
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);   //同时最多存在两个，因为有下一个参数的存在，所以这个参数永远也用不到
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);     //上一次的尾和下一次的头间隔大于2s

        //2. 读取Kafka 3个主题中的数据来创建流
        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));

        //3 将三个流转换为统一格式    (使用的是Unit方式)
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPageDS = pageDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            long sv = 0L;
            if (jsonObject.getJSONObject("page").getString("last_page_id") == null) {
                sv = 1L;
            }
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts"));
        });

        //4. union三个流
        DataStream<VisitorStats> unionDS = visitorStatsWithPageDS.union(visitorStatsWithUvDS, visitorStatsWithUjDS);

        //5. 提取时间生成waterMark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(4)).withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
            @Override
            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                return element.getTs();
            }
        }));
        
        //6. 分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWmDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getAr(),
                        value.getCh(),
                        value.getVc(),
                        value.getIs_new());
            }
        });

        //7. 开窗 聚合
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<VisitorStats> reduce = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());

                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                //获取数据
                VisitorStats visitorStats = input.iterator().next();

                //获取窗口信息
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                String stt = sdf.format(window.getStart());
                String edt = sdf.format(window.getEnd());

                //将窗口信息补充到数据中
                visitorStats.setStt(stt);
                visitorStats.setEdt(edt);

                //输出数据
                out.collect(visitorStats);
            }
        });
        //8.将数据写入ClickHouse
        reduce.print();

        reduce.addSink(ClickHouseUtil.getClickHouseSink(""));

        //9.启动任务
        env.execute();
    }
}
