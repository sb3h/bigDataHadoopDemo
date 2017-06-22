package com.template.mr.d10;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PKFKCompartor extends WritableComparator {

	protected PKFKCompartor() {
        super(UserKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		UserKey a1 = (UserKey)a;
		UserKey b1 = (UserKey)b;
		System.out.println(String.format("PKFKCompartor:compare:a1.getCityNo():%d==b1.getCityNo():%d"
				,a1.getCityNo(),b1.getCityNo()));
		if(a1.getCityNo()==b1.getCityNo()){
			return 0;
		}else{
			return a1.getCityNo()>b1.getCityNo()?1:-1;
		}
	}
	
}