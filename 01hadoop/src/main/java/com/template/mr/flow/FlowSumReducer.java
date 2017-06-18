package com.template.mr.flow;


import com.template.mr.flow.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by huanghh on 2017/6/5.
 */
public class FlowSumReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
        long flow_up = 0;
        long flow_down = 0;
        for (FlowBean value:values) {
//            count = count + value.get();
            flow_up = flow_up + value.getFlow_up();
            flow_down = flow_down + value.getFlow_down();
        }
        context.write(key,new FlowBean(key.toString(),flow_up,flow_down));
    }
}
