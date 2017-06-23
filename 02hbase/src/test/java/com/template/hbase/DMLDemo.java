package com.template.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
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
public class DMLDemo {
    Admin admin = null;
    @Before
    public void init() throws IOException {

//        admin = new HBaseAdmin(conf);
        admin = HBaseTools.getAdmin("zk01:2181,zk02:2181,zk03:2181");
    }


    @Test
    public void testScan() throws IOException {
//        Table table = admin.getConnection().getTable(TableName.valueOf("user"));
        Table table = new HTable(admin.getConfiguration(),"user");
        Scan s = new Scan();

        ResultScanner rs = table.getScanner(s);

        List<Map<String, Object>> resList = new ArrayList<Map<String, Object>>();
        for (Result r : rs) {
            Map<String, Object> tempmap = HBaseTools.resultToMap(r);
            resList.add(tempmap);
        }
        System.out.println(resList);

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
