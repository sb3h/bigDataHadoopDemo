package com.template.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by huanghh on 2017/6/13.
 */
public class DDlDemo {
    Admin admin = null;

    @Before
    public void init() throws IOException {

//        admin = new HBaseAdmin(conf);
        admin = HBaseTools.getAdmin("zk01:2181,zk02:2181,zk03:2181");
    }

    @Test
    public void testCreateTable() throws IOException {
//        System.out.println("123");
        HTableDescriptor descriptor = new HTableDescriptor("account");
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes("base_info"));
        columnDescriptor.setVersions(1,3);
        descriptor.addFamily(columnDescriptor);
        admin.createTable(descriptor);
    }

    @Test
    public void testCreateTableFromBook() throws IOException {
//        System.out.println("123");
        HTableDescriptor descriptor = new HTableDescriptor("e_table");
//        HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes("base_info"));
//        descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("ip:")));
        descriptor.addFamily(new HColumnDescriptor("ip"));
        descriptor.addFamily(new HColumnDescriptor("time"));
        descriptor.addFamily(new HColumnDescriptor("type"));
        descriptor.addFamily(new HColumnDescriptor("cookie"));
        descriptor.addFamily(new HColumnDescriptor("c"));
        admin.createTable(descriptor);
    }

    @Test
    public void testTableList() throws IOException {
//        TableName[] tableNames = admin.listTableNames();
        TableName[] tableNames = admin.listTableNames();
        for (int i = 0; i < tableNames.length; i++) {
            System.out.println(tableNames[i].getNameAsString());
        }
    }

    @Test
    public void testDescribe() throws IOException {
//        TableName[] tableNames = admin.listTableNames("user");
//        if (tableNames == null) return;
//        HTableDescriptor descriptor = admin.getTableDescriptor(tableNames[0]);
        HTableDescriptor descriptor = admin.getTableDescriptor(TableName.valueOf("user"));

        for (HColumnDescriptor hColumnDescriptor :descriptor.getColumnFamilies()) {
            System.out.println(hColumnDescriptor.getNameAsString());
            System.out.println(hColumnDescriptor.getMinVersions());
            System.out.println(hColumnDescriptor.getMaxVersions());
//            Map<ImmutableBytesWritable,ImmutableBytesWritable> map = hColumnDescriptor.getValues();
//            for (ImmutableBytesWritable writable:map.keySet()) {
//                System.out.println("new String(writable.get()):"+new String(writable.get()));
//            }
        }

    }

    @Test
    public void testRun() throws IOException {
//        System.out.println("123");

    }

    @After
    public void end() throws IOException {
        HBaseTools.close();
    }
}
