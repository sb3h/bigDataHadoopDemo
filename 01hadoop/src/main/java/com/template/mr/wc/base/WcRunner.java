package com.template.mr.wc.base;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by huanghh on 2017/6/6.
 */
public class WcRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WcRunner.class);
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        job.setMapOutputKeyClass(Text.class);
        //如果你的输出类型写错。你的reduce不会跑进去的
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path("hdfs://master-node:9000/test/mr/input/README.txt"));
        FileOutputFormat.setOutputPath(job,new Path(String.format("hdfs://master-node:9000/test/mr/output/base/%s",UUID.randomUUID())));

        job.waitForCompletion(true);
    }
}
