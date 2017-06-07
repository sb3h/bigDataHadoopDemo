package com.template.mr.wc.flow.partitioner;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by huanghh on 2017/6/6.
 */
public class FlowPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {

    public static Map<String, Integer> map = new HashMap<>();

    static {
        //可以从这里进行初始化，例如读取DB中的多级联动
        map.put("135", 0);
        map.put("136", 1);
        map.put("137", 2);
        map.put("138", 3);
        map.put("139", 4);
    }

    @Override
    public int getPartition(KEY key, VALUE value, int numPartitions) {
        String phone = key.toString();
        String sub = phone.substring(0, 3);
        Integer partition = 5;
        if (map.containsKey(sub)) {
            partition = map.get(sub);
        }
        return partition;
    }
}
