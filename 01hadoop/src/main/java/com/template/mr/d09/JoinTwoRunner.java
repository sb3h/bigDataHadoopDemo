package com.template.mr.d09;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by huanghh on 2017/6/6.
 * job提交标准写法
 */
public class JoinTwoRunner extends Configured implements Tool {

    public static final String demoPath = "JoinTwo";

    public static void main(String[] args) throws Exception {
//        ToolRunner tool = new ToolRunner();
//        tool.run(new JoinTwoRunner(),args);
        int run = ToolRunner.run(new JoinTwoRunner(), args);
        System.exit(run);
//        System.out.println(run);
    }

    //hdfs dfs -put file1 file2 /test/mr/input/JoinTwo

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(JoinTwoRunner.class);
        job.setMapperClass(MMapper.class);
        job.setReducerClass(MReducer.class);

//        如果map输出类型与reduce一样,可以不写map的
        job.setMapOutputKeyClass(IntWritable.class);
//        //如果你的输出类型写错。你的reduce不会跑进去的
        job.setMapOutputValueClass(User.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
//        hdfs dfs -cat /test/mr/input/JoinTwo/*
        FileInputFormat.setInputPaths(job, new Path(String.format("hdfs://ns/test/mr/input/%s/*",demoPath)));

//        hdfs dfs -rm -r /test/mr/output/JoinTwo/*
        //hdfs dfs -cat /test/mr/output/JoinTwo/*/*
        FileOutputFormat.setOutputPath(job, new Path(String.format("hdfs://ns/test/mr/output/%s/%s",demoPath, UUID.randomUUID())));


        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class MMapper extends Mapper<LongWritable, Text, IntWritable, User> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arr = line.split("\t");
            if(arr.length==2){//city
                User user = new User();
                user.setCityNo(arr[0]);
                user.setCityName(arr[1]);
                user.setFlag(0);
                context.write(new IntWritable(Integer.parseInt(arr[0])), user);
            }else{//user
                User user = new User();
                user.setUserNo(arr[0]);
                user.setUserName(arr[1]);
                user.setCityNo(arr[2]);
                user.setFlag(1);

                context.write(new IntWritable(Integer.parseInt(arr[2])), user);

            }
        }
    }

    private static class MReducer extends Reducer<IntWritable, User, NullWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<User> values,Context context)
                throws IOException, InterruptedException {
            User city = null;
            List<User> list = new ArrayList<User>();
            for(User u:values){
                if(u.getFlag()==0){
                    city = new User(u);
                }else{
                    list.add(new User(u));
                }
            }

            for(User u: list){
                u.setCityName(city.getCityName());
                context.write(NullWritable.get(), new Text(u.toString()));
            }

        }

    }
}
