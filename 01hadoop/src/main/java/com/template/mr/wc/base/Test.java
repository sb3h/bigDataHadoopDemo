package com.template.mr.wc.base;

import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by huanghh on 2017/6/18.
 */
public class Test {
    public static void main(String[] args) {
//        AtomicLong atomicLong = new AtomicLong();
//        for (int i = 0; i < 100; i++) {
//            System.out.println(atomicLong.addAndGet(1));;
////            System.out.println(atomicLong.get());
//        }
        String tokenizer = new String("张三\t80");
        for(String str :tokenizer.split("\t")){
            System.out.println(str);
        };
    }
}
