package com.template.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created by sam3h on 2016/7/5 0005.
 */
public class JunitHDFS {
    FileSystem fs = null;

    @Before
    public void init() throws IOException {
        Configuration conf = new Configuration();//此处的配置是因为你的resources有core-site.xml，hdfs-site.xml进行配置，不然默认是读取local

        fs = FileSystem.get(conf);
    }

    @Test
    public void testPrintRootFile() throws IOException {
        RemoteIterator<LocatedFileStatus> itor = fs.listFiles(new Path("/user"), true);
        while (itor.hasNext()) {
            LocatedFileStatus fileStatus = itor.next();
            String name = fileStatus.getPath().getName();
            System.out.println(fileStatus.getPath().getParent());
//            System.out.println(name);
        }
    }

    @Test
    public void testReadFile() throws IOException {
        String uri = "hdfs://ns/test/mr/input/order/file1";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        printFileContentByUri(uri, fs);
    }

    public static void printFileContentByUri(String uri, FileSystem fs) throws IOException {
        printFileContentByUri(uri, fs,System.out);
    }

    public static void printFileContentByUri(String uri, FileSystem fs,OutputStream out) throws IOException {
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    @Test
    public void testReadDirFile() throws IOException {
        String uri = "hdfs://ns/test/mr/input/order";
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(uri), conf);
        FileStatus[] fs = hdfs.listStatus(new Path(uri));
        Path[] listPath = FileUtil.stat2Paths(fs);
        for (Path p : listPath){
//            System.out.println(p);
            printFileContentByUri(p.toString(),hdfs);
        }
    }


    @After
    public void end() throws IOException {
        fs.close();
    }
}
