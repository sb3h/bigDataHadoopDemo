package com.template.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by huanghh on 2017/6/13.
 */
public class HBaseDemo {
    Configuration conf = null;
    HBaseAdmin admin = null;
    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zk01:2181,zk02:2181,zk03:2181");
        admin = new HBaseAdmin(conf);
    }

    @Test
    public void testCreateTable() throws IOException {
//        System.out.println("123");
        HTableDescriptor descriptor = new HTableDescriptor("account");
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes("base_info"));
        descriptor.addFamily(columnDescriptor);
        admin.createTable(descriptor);
    }

    @Test
    public void testRun() throws IOException {
//        System.out.println("123");

    }

    @After
    public void end() throws IOException {
        admin.close();
    }
}
