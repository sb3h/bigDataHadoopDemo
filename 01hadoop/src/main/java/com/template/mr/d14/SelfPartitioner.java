package com.template.mr.d14;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SelfPartitioner extends Partitioner<IntPair, IntWritable> {

	@Override
    public int getPartition(IntPair paramKEY, IntWritable paramVALUE,
			int paramInt) {
		return Math.abs(paramKEY.getFirstKey().hashCode()*127)%paramInt;
	}

}