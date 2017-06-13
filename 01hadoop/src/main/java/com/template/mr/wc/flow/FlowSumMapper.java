package com.template.mr.wc.flow;


import com.template.mr.wc.flow.bean.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by huanghh on 2017/6/5.
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        String line = value.toString();
//        String[] words = line.split(" ");
//        String[] words = StringUtils.split(line,'\t');

        FlowBean bean = new FlowBean(line);

        context.write(new Text(bean.getPhone()), bean);
    }
}
