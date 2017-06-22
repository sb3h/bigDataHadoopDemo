package com.template.mr.d14;


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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by huanghh on 2017/6/6.
 * job提交标准写法
 */
public class ManyColumnRunner extends Configured implements Tool {

    public static final String demoPath = "ManyColumn";

    public static void main(String[] args) throws Exception {
//        ToolRunner tool = new ToolRunner();
//        tool.run(new tempRunner(),args);
        int run = ToolRunner.run(new ManyColumnRunner(), args);
        System.exit(run);
//        System.out.println(run);
    }

    //hdfs dfs -put file1 file2 /test/mr/input/ManyColumn

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ManyColumnRunner.class);


        job.setMapperClass(MMapper.class);
//        如果map输出类型与reduce一样,可以不写map的
        job.setMapOutputKeyClass(IntPair.class);
//        //如果你的输出类型写错。你的reduce不会跑进去的
        job.setMapOutputValueClass(IntWritable.class);
        job.setPartitionerClass(SelfPartitioner.class);
        job.setSortComparatorClass(IntSortComparator.class);
//        hdfs dfs -cat /test/mr/input/ManyColumn/*
        FileInputFormat.setInputPaths(job, new Path(String.format("hdfs://ns/test/mr/input/%s/*", demoPath)));

        job.setCombinerClass(MReducer.class);

        job.setGroupingComparatorClass(GroupingComparator.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setReducerClass(MReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        hdfs dfs -rm -r /test/mr/output/ManyColumn/*
        //hdfs dfs -cat /test/mr/output/ManyColumn/*/*
        FileOutputFormat.setOutputPath(job, new Path(String.format("hdfs://ns/test/mr/output/%s/%s", demoPath, UUID.randomUUID())));


        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class MMapper extends Mapper<Object, Text, IntPair, IntWritable> {

        @Override
        protected void map(Object key, Text value,Context context)
                throws IOException, InterruptedException {
            String first = key.toString().trim();
            Integer second = Integer.parseInt(value.toString().trim());
            context.write(new IntPair(first,second), new IntWritable(second));
        }
    }

    private static class MReducer extends
            Reducer<IntPair, IntWritable, Text, Text> {

        @Override
        protected void reduce(IntPair key, Iterable<IntWritable> values,Context context)
                throws IOException, InterruptedException {
            StringBuffer sBuffer = new StringBuffer();
            for(IntWritable val : values){
                sBuffer.append(val.get()+",");
            }
            int length = sBuffer.toString().length();
            String value = sBuffer.toString().substring(0,length-1);

            context.write(new Text(key.getFirstKey()), new Text(value));

        }
    }
}
