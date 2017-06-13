package com.template.mr.wc.flow;


import com.template.mr.wc.flow.bean.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by huanghh on 2017/6/6.
 * job提交标准写法
 */
public class FlowSumRunner extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
//        ToolRunner tool = new ToolRunner();
//        tool.run(new FlowSumRunner(),args);
        int run = ToolRunner.run(new FlowSumRunner(), args);
        System.exit(run);
//        System.out.println(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSumRunner.class);
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

//        如果map输出类型与reduce一样,可以不写map的
//        job.setMapOutputKeyClass(Text.class);
//        //如果你的输出类型写错。你的reduce不会跑进去的
//        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
//        hdfs dfs -rm -r /test/mr/output/flow/*
//        hdfs dfs -ls /test/mr/output/flow/*
//        hdfs dfs -cat /test/mr/output/flow/*/*
        FileInputFormat.setInputPaths(job, new Path("hdfs://master-node:9000/test/mr/input/HTTP_20130313143750.dat"));
        FileOutputFormat.setOutputPath(job, new Path(String.format("hdfs://master-node:9000/test/mr/output/flow/%s", UUID.randomUUID())));


        return job.waitForCompletion(true) ? 0 : 1;
    }
}
