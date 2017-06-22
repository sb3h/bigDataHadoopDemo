package com.template.mr.d11;


import com.template.mr.d11.GroupingCompartor;
import com.template.mr.d11.IntPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by huanghh on 2017/6/6.
 * job提交标准写法
 */
public class SecSortRunner extends Configured implements Tool {

    public static final String demoPath = "SecSort";

    public static void main(String[] args) throws Exception {
//        ToolRunner tool = new ToolRunner();
//        tool.run(new SecSortRunner(),args);
        int run = ToolRunner.run(new SecSortRunner(), args);
        System.exit(run);
//        System.out.println(run);
    }

    //hdfs dfs -put file1 file2 /test/mr/input/SecSort

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SecSortRunner.class);


        job.setMapperClass(MMapper.class);
//        如果map输出类型与reduce一样,可以不写map的
        job.setMapOutputKeyClass(IntPair.class);
//        //如果你的输出类型写错。你的reduce不会跑进去的
        job.setMapOutputValueClass(Text.class);
//        hdfs dfs -cat /test/mr/input/SecSort/*
        FileInputFormat.setInputPaths(job, new Path(String.format("hdfs://ns/test/mr/input/%s/file1", demoPath)));

//        job.setCombinerClass(MReducer.class);
        job.setGroupingComparatorClass(GroupingCompartor.class);//

//        job.setPartitionerClass(FirstPartitioner.class);

        job.setReducerClass(MReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
        job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出。
        job.setOutputFormatClass(TextOutputFormat.class);
//        hdfs dfs -rm -r /test/mr/output/SecSort/*
        //hdfs dfs -cat /test/mr/output/SecSort/*/*
        FileOutputFormat.setOutputPath(job, new Path(String.format("hdfs://ns/test/mr/output/%s/%s", demoPath, UUID.randomUUID())));


        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class MMapper extends Mapper<LongWritable, Text, IntPair, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("map");
            String line = value.toString();
            String[] arr = line.split("\t");
            if(arr.length==3){
                IntPair tmp = new IntPair(arr[0],arr[1]);
                context.write(tmp, value);
            }
        }
    }

    private static class MReducer extends Reducer<IntPair, Text, NullWritable, Text> {
        private static Text SEP = new Text("-----------------------------------");
        @Override
        protected void reduce(IntPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("reduce");
            context.write(NullWritable.get(), SEP);
            for(Text val:values){
                context.write(NullWritable.get(), val);
            }
        }
    }
}
