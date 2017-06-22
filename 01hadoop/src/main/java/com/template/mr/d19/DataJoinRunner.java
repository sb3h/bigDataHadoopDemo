package com.template.mr.d19;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * Created by huanghh on 2017/6/6.
 * job提交标准写法
 */
public class DataJoinRunner extends Configured implements Tool {

    public static final String demoPath = "DataJoin";

    public static void main(String[] args) throws Exception {
//        ToolRunner tool = new ToolRunner();
//        tool.run(new tempRunner(),args);
        int run = ToolRunner.run(new DataJoinRunner(), args);
        System.exit(run);
//        System.out.println(run);
    }

    //hdfs dfs -put file1 file2 /test/mr/input/DataJoin

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        JobConf jobConf = new JobConf(conf,DataJoinRunner.class);

        jobConf.setJarByClass(DataJoinRunner.class);


        jobConf.setMapperClass(MMapper.class);
//        如果map输出类型与reduce一样,可以不写map的
//        jobConf.setMapOutputKeyClass(Text.class);
////        //如果你的输出类型写错。你的reduce不会跑进去的
//        jobConf.setMapOutputValueClass(Text.class);
//        hdfs dfs -cat /test/mr/input/DataJoin/*


//        jobConf.setCombinerClass(MReducer.class);

        jobConf.setReducerClass(MReducer.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        jobConf.set("mapred.textoutputformat.separator",",");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(TaggedWritable.class);
//        hdfs dfs -rm -r /test/mr/output/DataJoin/*
        //hdfs dfs -cat /test/mr/output/DataJoin/*/*

        Job job = Job.getInstance(jobConf);

        FileInputFormat.setInputPaths(job, new Path(String.format("hdfs://ns/test/mr/input/%s/*", demoPath)));
        FileOutputFormat.setOutputPath(job, new Path(String.format("hdfs://ns/test/mr/output/%s/%s", demoPath, UUID.randomUUID())));


        return job.waitForCompletion(true) ? 0 : 1;
    }

    //TaggedMapOutput是一个抽象数据类型，封装了标签与记录内容
    //此处作为DataJoinMapperBase的输出值类型，需要实现Writable接口，所以要实现两个序列化方法
    public static class TaggedWritable extends TaggedMapOutput
    {
        private Writable data ;

        public TaggedWritable() {
            this(new Text(""));
        }

        public TaggedWritable(Writable data)    //构造函数
        {
            this.tag = new Text();      //tag可以通过setTag()方法进行设置
            this.data = data;
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            tag.readFields(in);
            if (data != null) {
                data.readFields(in);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            tag.write(out);
            if (data != null) {
                data.write(out);
            }
        }

        @Override
        public Writable getData() {
            return data;
        }

    }

    private static class MMapper extends DataJoinMapperBase{

        //这个在任务开始时调用，用于产生标签
        //此处就直接以文件名作为标签
        @Override
        protected Text generateInputTag(String inputFile) {
            return new Text(inputFile);
        }

        //这里我们已经确定分割符为','，更普遍的，用户应能自己指定分割符和组键。
        @Override
        protected Text generateGroupKey(TaggedMapOutput record) {
            String line = ((Text)record.getData()).toString();
            String[] tokens = line.split(",");
            return new Text(tokens[0]);
        }

        @Override
        protected TaggedMapOutput generateTaggedMapOutput(Object value) {
            TaggedWritable retv = new TaggedWritable((Text) value);
            retv.setTag(this.inputTag);     //不要忘记设定当前键值的标签
            return retv;
        }
    }

    private static class MReducer extends DataJoinReducerBase
    {
        //两个参数数组大小一定相同，并且最多等于数据源个数
        @Override
        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            if (tags.length < 2) return null;
            if (values == null) {
                return null;
            }
            String joinedStr = "";
            for (int i=0; i<values.length; i++) {
                if (i > 0) joinedStr += ",";
                TaggedWritable tw = (TaggedWritable) values[i];
                String line = ((Text) tw.getData()).toString();
                String[] tokens = line.split(",", 2);
                joinedStr += tokens[1];
            }
            TaggedWritable retv = new TaggedWritable(new Text(joinedStr));
            retv.setTag((Text) tags[0]);
            return retv;
        }
    }
}
