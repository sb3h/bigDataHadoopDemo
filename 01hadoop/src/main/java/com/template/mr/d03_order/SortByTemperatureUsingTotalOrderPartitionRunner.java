package com.template.mr.d03_order;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
public class SortByTemperatureUsingTotalOrderPartitionRunner extends Configured implements Tool {

    public static final String demoPath = "order";

    public static void main(String[] args) throws Exception {
//        ToolRunner tool = new ToolRunner();
//        tool.run(new orderRunner(),args);
        int run = ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitionRunner(), args);
        System.exit(run);
//        System.out.println(run);
    }

    //hdfs dfs -put file1 file2 /test/mr/input/order

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SortByTemperatureUsingTotalOrderPartitionRunner.class);
        job.setMapperClass(MMapper.class);
//        job.setCombinerClass(MReducer.class);
        job.setReducerClass(MReducer.class);

//        如果map输出类型与reduce一样,可以不写map的
        job.setMapOutputKeyClass(IntWritable.class);
        //如果你的输出类型写错。你的reduce不会跑进去的
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
//        hdfs dfs -cat /test/mr/input/order/*
        FileInputFormat.setInputPaths(job, new Path(String.format("hdfs://ns/test/mr/input/%s/*",demoPath)));

//        hdfs dfs -rm -r /test/mr/output/order/*
//        hdfs dfs -cat /test/mr/output/order/*/*
        FileOutputFormat.setOutputPath(job, new Path(String.format("hdfs://ns/test/mr/output/%s/%s",demoPath, UUID.randomUUID())));


        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class MMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private Text val = new Text("");
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if(line.trim().length()>0){
                context.write(new IntWritable(Integer.valueOf(line.trim())), val);
            }
        }

    }

    private static class MReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

        private IntWritable num = new IntWritable(1);
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            for(Text val:values){//之所以要遍历，避免有重复的数据没有进行排序，
                context.write(num, key);
                num = new IntWritable(num.get()+1);
            }

        }
    }
}
