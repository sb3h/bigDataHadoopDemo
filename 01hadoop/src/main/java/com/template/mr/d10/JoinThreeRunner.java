package com.template.mr.d10;


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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by huanghh on 2017/6/6.
 * job提交标准写法
 */
public class JoinThreeRunner extends Configured implements Tool {

    public static final String demoPath = "JoinThree";

    public static void main(String[] args) throws Exception {
//        ToolRunner tool = new ToolRunner();
//        tool.run(new JoinThreeRunner(),args);
        int run = ToolRunner.run(new JoinThreeRunner(), args);
        System.exit(run);
//        System.out.println(run);
    }

    //hdfs dfs -put file1 file2 /test/mr/input/JoinThree

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(JoinThreeRunner.class);
        job.setMapperClass(MMapper.class);

//        hdfs dfs -cat /test/mr/input/JoinThree/*
        FileInputFormat.setInputPaths(job, new Path(String.format("hdfs://ns/test/mr/input/%s/*", demoPath)));
//        如果map输出类型与reduce一样,可以不写map的
        job.setMapOutputKeyClass(UserKey.class);
//        //如果你的输出类型写错。你的reduce不会跑进去的
        job.setMapOutputValueClass(User.class);

        job.setGroupingComparatorClass(PKFKCompartor.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(MReducer.class);

//        hdfs dfs -rm -r /test/mr/output/JoinThree/*
        //hdfs dfs -cat /test/mr/output/JoinThree/*/*
        FileOutputFormat.setOutputPath(job, new Path(String.format("hdfs://ns/test/mr/output/%s/%s", demoPath, UUID.randomUUID())));


        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class MMapper extends Mapper<LongWritable, Text, UserKey, User> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arr = line.split("\t");
            int len = arr.length;
            if (len == 2) {//city
                User user = new User();
                user.setCityNo(arr[0]);
                user.setCityName(arr[1]);

                UserKey uKey = new UserKey();
                uKey.setCityNo(Integer.parseInt(arr[0]));
                uKey.setPrimary(true);

                context.write(uKey, user);
            } else if (len >2){//user
                User user = new User();
                user.setUserNo(arr[0]);
                user.setUserName(arr[1]);
                user.setCityNo(arr[2]);

                UserKey uKey = new UserKey();
                uKey.setCityNo(Integer.parseInt(arr[2]));
                uKey.setPrimary(false);

                context.write(uKey, user);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("cleanup-map");
            super.cleanup(context);
        }
    }

    private static class MReducer extends Reducer<UserKey, User, NullWritable, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println("reduce-setup");
            super.setup(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("reduce-cleanup");
            super.cleanup(context);
        }

        @Override
        protected void reduce(UserKey key, Iterable<User> values, Context context) throws IOException, InterruptedException {
            System.out.println("reduce");
            User user = null;
            int num = 0;
            for (User u : values) {
                if (num == 0) {
                    user = new User(u);//第一次进来，肯定是city的
                    num++;
                } else {
                    u.setCityName(user.getCityName());
                    context.write(NullWritable.get(), new Text(u.toString()));
                }
            }
        }
    }
}
