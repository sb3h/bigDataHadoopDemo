package com.template.mr.wc.flow.partitioner;


import com.template.mr.wc.flow.bean.FlowBean;
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
 */
public class FlowPartitionerRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new FlowPartitionerRunner(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();

        job.setJarByClass(FlowPartitionerRunner.class);
        job.setMapperClass(FlowPartitionerM.class);
        job.setReducerClass(FlowPartitionerR.class);
        //--------------------------job Partitioner start ---------------------------------
        job.setPartitionerClass(FlowPartitioner.class);
//        int num = FlowPartitioner.map!=null&&FlowPartitioner.map.size()>0?FlowPartitioner.map.size():1;
//        //设置reduce task数量
//        job.setNumReduceTasks(num+1);//肯定有预料不到的分区，所以加1
//        job.setNumReduceTasks(0);/**就直接不走reduce了，直接map就输出了*/
//        job.setNumReduceTasks(1);/**如果分区数没有发生(等于1),那就不会进入reduce和getPartition,直接调用HashPartitioner*/
//        job.setNumReduceTasks(5);/**当分区返回时，发生错误，整个MR都崩溃*/
//        job.setNumReduceTasks(10);/**当分区数大于指定分区数时，多出来的分区就会没有内容*/
        job.setNumReduceTasks(6);
        //--------------------------job Partitioner end ---------------------------------
//        如果map输出类型与reduce一样,可以不写map的
//        job.setMapOutputKeyClass(FlowBean.class);
//        //如果你的输出类型写错。你的reduce不会跑进去的
//        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //一定要注意使用的序列化，一定有writable的，不然是进不去的
        FileInputFormat.setInputPaths(job, new Path("hdfs://master-node:9000/test/mr/output/flow/72c38e23-6d42-44ab-8d68-cde83542c36b/part-r-00000"));
//        hdfs dfs -mkdir /test/mr/output/partitioner
//        hdfs dfs -ls -R /test/mr/output/partitioner
//        hdfs dfs -rm -r -f /test/mr/output/partitioner/*
//        hdfs dfs -cat /test/mr/output/partitioner/*/*
        FileOutputFormat.setOutputPath(job, new Path(String.format("hdfs://master-node:9000/test/mr/output/partitioner/%s", UUID.randomUUID())));


        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class FlowPartitionerR extends Reducer<Text, FlowBean, Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
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

    private static class FlowPartitionerM extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
            String line = value.toString();
//        String[] words = line.split(" ");
//            String[] words = StringUtils.split(line, '\t');

            FlowBean bean = new FlowBean(line, false);

            context.write(new Text(bean.getPhone()), bean);
        }
    }
}
