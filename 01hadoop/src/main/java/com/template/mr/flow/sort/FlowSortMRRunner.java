package com.template.mr.flow.sort;


import com.template.mr.flow.bean.FlowBean;
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
public class FlowSortMRRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new FlowSortMRRunner(),args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();

        job.setJarByClass(FlowSortMRRunner.class);
        job.setMapperClass(FlowSortM.class);
        job.setReducerClass(FlowSortR.class);

//        如果map输出类型与reduce一样,可以不写map的
        job.setMapOutputKeyClass(FlowBean.class);
        //如果你的输出类型写错。你的reduce不会跑进去的
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //一定要注意使用的序列化，一定有writable的，不然是进不去的
        FileInputFormat.setInputPaths(job, new Path("hdfs://master-node:9000/test/mr/output/flow/72c38e23-6d42-44ab-8d68-cde83542c36b/part-r-00000"));
//        hdfs dfs -mkdir /test/mr/output/sorted
//        hdfs dfs -ls -R /test/mr/output/sorted
//        hdfs dfs -rm -r -f /test/mr/output/sorted/*
//        hdfs dfs -cat /test/mr/output/sorted/*/*
        FileOutputFormat.setOutputPath(job, new Path(String.format("hdfs://master-node:9000/test/mr/output/sorted/%s", UUID.randomUUID())));


        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class FlowSortR extends Reducer<FlowBean, NullWritable, Text, FlowBean> {
        @Override
        protected void reduce(FlowBean bean, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
//            long flow_up = 0;
//            long flow_down = 0;
//            for (FlowBean value : values) {
////            count = count + value.get();
//                flow_up = flow_up + value.getFlow_up();
//                flow_down = flow_down + value.getFlow_down();
//            }
            context.write(new Text(bean.getPhone()), bean);
        }
    }

    private static class FlowSortM extends Mapper<LongWritable, Text, FlowBean, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
            String line = value.toString();
//        String[] words = line.split(" ");
//            String[] words = StringUtils.split(line, '\t');

            FlowBean bean = new FlowBean(line,false);

            context.write(bean, NullWritable.get());
        }
    }
}
