package com.template.mr.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by sam3h on 2016/7/5 0005.
 */
public class JunitMRFileRead {
    public static final String HDFS_NS = "hdfs://ns";
    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init() throws IOException {
        conf = new Configuration();//此处的配置是因为你的resources有core-site.xml，hdfs-site.xml进行配置，不然默认是读取local

        fs = FileSystem.get(conf);
    }

    public static final String[] myVal = {
        "hello world",
        "bye world",
        "hello hadoop",
        "bye hadoop",
    };

    WritableComparable key = new IntWritable();
    WritableComparable value = new Text();

    Closeable mWriter = null;



    @Test
    public void testSequenceFileWriteFileBlock() throws IOException {
        String outDirStr = String.format("%s/data/SequenceFileBlock", HDFS_NS);
        Path outDirPath = new Path(outDirStr);

        SequenceFile.Writer writer = SequenceFile.createWriter(fs,conf,outDirPath,key.getClass(),value.getClass());
        mWriter = writer;

        for (int i = 0; i < 50000; i++) {
            ((IntWritable)key).set(50000-i);
            ((Text)value).set(myVal[i%myVal.length]);
            writer.append(key,value);
        }
    }


    @After
    public void end() throws IOException {

        IOUtils.closeStream(mWriter);
        fs.close();
    }
}
