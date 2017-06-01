package com.template;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by sam3h on 2016/7/5 0005.
 */
public class HelloJunit {
    FileSystem fs = null;
    @Before
    public void init() throws IOException {
        Configuration conf = new Configuration();//此处的配置是因为你的resources有core-site.xml，hdfs-site.xml进行配置，不然默认是读取local
        fs = FileSystem.get(conf);
    }

    @Test
    public void printRootFile() throws IOException {
        RemoteIterator<LocatedFileStatus> itor = fs.listFiles(new Path("/"), true);
        while (itor.hasNext()) {
            LocatedFileStatus fileStatus = itor.next();
            String name = fileStatus.getPath().getName();
            System.out.println(fileStatus.getPath().getParent());
            System.out.println(name);
        }
    }

    @After
    public void end() throws IOException {
        fs.close();
    }
}
