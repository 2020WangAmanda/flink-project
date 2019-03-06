package com.wangran.firstflink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopNHotItem extends KeyedProcessFunction<Tuple, ItemViewCount,String> {
   private final int topSize;
   private ListState<ItemViewCount> itemState;//用于存储商品与点击数的状态，待收齐同一个窗口的数据后 再触发TopN计算
    public TopNHotItem(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //状态的注册
        super.open(parameters);
        //状态的注册
        ListStateDescriptor<ItemViewCount> itemViewCountListStateDescriptor
                = new ListStateDescriptor<ItemViewCount>("itemState-state",ItemViewCount.class);
        itemState =getRuntimeContext()
                .getListState(itemViewCountListStateDescriptor);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<ItemViewCount> allItems = new ArrayList<>();
        for (ItemViewCount item: itemState.get()) {
            allItems.add(item);
        }
        itemState.clear();

        allItems.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int) (o2.viewCount-o1.viewCount);
            }
        });
        StringBuilder result = new StringBuilder();
        result.append("==================");
        result.append("Top3：").append(new Timestamp(timestamp-1)).append("\n");
        for (int i = 0; i <topSize ; i++) {
            ItemViewCount currentItem = allItems.get(i);
            result.append(i+1).append(":").append(currentItem.itemId).append("\n");

        }
        result.append("================");
        out.collect(result.toString());
    }

    @Override
    public void processElement(ItemViewCount input,
                               Context context, Collector<String> collector) throws Exception {
       //每条数据都保存到状态中
        itemState.add(input);
        context.timerService().registerEventTimeTimer(input.windowEnd+1);

    }
}
