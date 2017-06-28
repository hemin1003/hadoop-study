package com.hadoop.minbo.mapreduce.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class KeySort implements WritableComparable<KeySort>{

	public long key;
	public long value;
	
	public KeySort() { }

	public KeySort(long key, long value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(key);
		out.writeLong(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.key=in.readLong();
		this.value=in.readLong();
	}
	
//	/**
//	 * 当第一列不同时，升序；当第一列相同时，第二列升序
//	 */
//	@Override
//	public int compareTo(KeySort my) {
//		long temp=this.key-my.key;
//		if(temp!=0){
//			return (int) temp;
//		}
//		return (int) (this.value-my.value);
//	}
	
	/**
	 * 第一列：降序，当第一列相同时，第二列：升序
	 */
	@Override
	public int compareTo(KeySort my) {
		long temp=this.key-my.key;
		if(temp>0){
			temp=-1;
			return (int) temp;
		}else if(temp<0){
			temp=1;
			return (int) temp;
		}
		return (int) (this.value-my.value);
	}
}