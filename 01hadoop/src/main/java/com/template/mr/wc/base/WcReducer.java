package com.template.mr.wc.base;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by huanghh on 2017/6/5.
 */
public class WcReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
        long count = 0;
        for (LongWritable value:values) {
//            count = count + value.get();
            count = count + 1;
        }
        context.write(key,new LongWritable(count));
    }
}
