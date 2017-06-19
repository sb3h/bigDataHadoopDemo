package com.template.mr.d02_order;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
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
        args = new String[]{
                String.format("hdfs://ns/test/mr/input/%s/*",demoPath),
                String.format("hdfs://ns/test/mr/output/%s/%s",demoPath, UUID.randomUUID())
        };

        int run = ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitionRunner(), args);
        System.exit(run);
//        System.out.println(run);
    }

    //hdfs dfs -put file1 file2 /test/mr/input/order

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.set(FileInputFormat.INPUT_DIR,
//                String.format("hdfs://ns/test/mr/input/%s/file1,hdfs://ns/test/mr/input/%s/file2,hdfs://ns/test/mr/input/%s/file3",
//                        demoPath,
//                        demoPath,
//                        demoPath));


        Job job = Job.getInstance(conf);

//        job.setInputFormatClass(SequenceFileInputFormat.class);


//        job.setOutputKeyClass(IntWritable.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
////        job.setOutputValueClass(Text.class);有上面就不用这句了
//        SequenceFileOutputFormat.setCompressOutput(job,true);
//        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        FileOutputFormat.setOutputPath(job, new Path(String.format("hdfs://ns/test/mr/output/%s/%s",demoPath, UUID.randomUUID())));

        job.setPartitionerClass(TotalOrderPartitioner.class);

        InputSampler.Sampler<IntWritable,Text> sampler = new InputSampler.RandomSampler<IntWritable,Text>(0.1,10000,10);
        //输入
//        Path input = FileInputFormat.getInputPaths(job)[0];
        FileInputFormat.setInputPaths(job,String.format("hdfs://ns/test/mr/input/%s/*",demoPath));
        Path input = new Path(String.format("hdfs://ns/test/mr/input/%s/*",demoPath));
        input = input.makeQualified(input.getFileSystem(conf));
        //分区
        Path partitionFile = new Path(input,"_partition");
        TotalOrderPartitioner.setPartitionFile(conf,partitionFile);
        InputSampler.writePartitionFile(job,sampler);

        //添加到distributeCache中
        URI partitionUri = URI.create(partitionFile.toString() + "#partitions");
        DistributedCache.addCacheFile(partitionUri,conf);
        DistributedCache.createSymlink(conf);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class MMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String line = value.toString();
            context.write(value,new Text());
        }
    }

    private static class MReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,new Text(""));
        }
    }
}
