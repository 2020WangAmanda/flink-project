package com.wangran.firstflink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.net.URL;

public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));

        PojoTypeInfo<UserBehavior> pojoTypeInfo = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo( UserBehavior.class);
        String[] fieldOrder =new String[]{"userId","itemId","categoryId","behavior","timestamp"};
        PojoCsvInputFormat<UserBehavior> csvInput =new PojoCsvInputFormat<>(filePath,pojoTypeInfo,fieldOrder);

        //create dataSource
        DataStream<UserBehavior> dataSource =env.createInput(csvInput, pojoTypeInfo);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<UserBehavior> timedData = dataSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.timestamp*1000;
            }
        });
        DataStream<UserBehavior> pvData = timedData.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.behavior.equals("pv");
            }
        });
        DataStream<ItemViewCount> windowedData= pvData
                .keyBy("itemId")
                .timeWindow(Time.minutes(60),Time.minutes(5))
                .aggregate(new CountAgg(),new WindowResultFunction());
        DataStream<String> topItems=windowedData
                .keyBy("windowEnd")
                .process(new TopNHotItem(3));

        topItems.print();
        env.execute(" TopN3");

    }
}
