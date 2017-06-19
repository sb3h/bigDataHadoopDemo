package com.template.mr.file;

import com.template.hdfs.JunitHDFS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by sam3h on 2016/7/5 0005.
 */
public class JunitMRFileWrite {
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
    public void testMapFileWriteFile() throws IOException {
        String outDirStr = String.format("%s/data/MapFile", HDFS_NS);

        MapFile.Writer writer = new MapFile.Writer(conf,fs,outDirStr,key.getClass(),value.getClass());
        mWriter = writer;

        for (int i = 0; i < 500; i++) {
            ((IntWritable)key).set(i);
            ((Text)value).set(myVal[i%myVal.length]);
            ((MapFile.Writer)writer).append(key,value);
        }
    }

    @Test
    public void testSequenceFileWriteFile() throws IOException {
        String outDirStr = String.format("%s/data/SequenceFile", HDFS_NS);
        Path outDirPath = new Path(outDirStr);

        SequenceFile.Writer writer = SequenceFile.createWriter(fs,conf,outDirPath,key.getClass(),value.getClass());
        mWriter = writer;

        for (int i = 0; i < 50000; i++) {
            ((IntWritable)key).set(50000-i);
            ((Text)value).set(myVal[i%myVal.length]);
            writer.append(key,value);
        }
    }

    @Test
    public void testSequenceFileWriteFileByHDFSPath() throws IOException {
        String inDirStr = String.format("%s/test/mr/input/order/file3", HDFS_NS);
        String outDirStr = String.format("%s/test/mr/input/order_seq", HDFS_NS);
        Path outDirPath = new Path(outDirStr);

        FileSystem hdfs;
        RecordReader<LongWritable, Text> reader ;
        SequenceFile.Writer writer = null ;
        try {
            hdfs = (DistributedFileSystem) FileSystem.get(conf);
            writer = SequenceFile.createWriter(hdfs, conf, outDirPath,
                    Text.class, Text.class, SequenceFile.CompressionType.NONE);

            FSDataInputStream dis = hdfs.open(new Path(inDirStr));
            reader  = new LineRecordReader(dis,0,  hdfs.getFileStatus(new Path(inDirStr)).getLen() , conf) ;
            LongWritable key = new LongWritable();
            Text value = reader.createValue();
            AtomicLong atomicLong = new AtomicLong();
            while(reader.next(key, value))
            {
                String[] keyValue = value.toString().split(" ") ;
                if(keyValue.length>0){
                    writer.append(new Text(String.valueOf(atomicLong.addAndGet(1))), new Text(keyValue[0]));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
        }


    }



    @Test
    public void testSequenceFileWriteFileBlock() throws IOException {
        String outDirStr = String.format("%s/data/SequenceFileBlock", HDFS_NS, SequenceFile.CompressionType.BLOCK);
        Path outDirPath = new Path(outDirStr);

        SequenceFile.Writer writer = SequenceFile.createWriter(fs,conf,outDirPath,key.getClass(),value.getClass());
        mWriter = writer;

        for (int i = 0; i < 50000; i++) {
            ((IntWritable)key).set(50000-i);
            ((Text)value).set(myVal[i%myVal.length]);
            writer.append(key,value);
        }
    }




    //hdfs dfs -rmr /data/*
    @After
    public void end() throws IOException {

        IOUtils.closeStream(mWriter);
        fs.close();
    }
}
