package com.template.mr;

import java.util.Arrays;

/**
 * Created by huanghh on 2017/6/21.
 */
public class HelloWorld {
    public static void main(String[] args) {
//        int len = 5;
//        int [] top = new int[len+1];
//        top[0] = 1;
//        Arrays.sort(top);
        String str = "1,Stephanie Leung,555-555-5555";
        String[] strs = str.split(",",4);
        for(String s:strs){
            System.out.println(s);
        }
//        System.out.println();
    }
}
