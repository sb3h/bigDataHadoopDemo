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
    public void testSequenceFileReadFile() throws IOException {
        String inDirStr = String.format("%s/data/SequenceFile", HDFS_NS);
        Path inDirPath = new Path(inDirStr);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs,inDirPath,conf);

        key = (WritableComparable)ReflectionUtils.newInstance(reader.getKeyClass(),conf);
        value = (WritableComparable)ReflectionUtils.newInstance(reader.getValueClass(),conf);

        long position = reader.getPosition();

        while (reader.next(key,value)){
            System.out.println(String.format("[%s-%s]%s %s",position,reader.syncSeen(),key,value));
            position = reader.getPosition();
        }
    }


    @After
    public void end() throws IOException {

        IOUtils.closeStream(mWriter);
        fs.close();
    }
}
