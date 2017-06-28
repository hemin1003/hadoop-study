package com.hadoop.minbo.mapreduce.custom2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 自定义数据类型需要在hadoop中传输需要实现Writable接口
 * 
 * @author MINBO
 */
public class MyData implements Writable {

	public long upPayLoad; // 上行流量
	public long downPayLoad; // 下行流量
	public long loadSum; // 总流量

	// 为了能够反序列化必须要定义一个无参数的构造函数
	public MyData() {

	}

	public MyData(long upPayLoad, long downPayLoad) {
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
		this.loadSum = upPayLoad + downPayLoad;// 利用构造函数的技巧，创建构造函数时，总流量被自动求出
	}

	// 只要数据在网络中进行传输，就需要序列化与反序列化
	// 先序列化,将对象(字段)写到字节输出流当中
	@Override
	public void write(DataOutput fw) throws IOException {
		fw.writeLong(upPayLoad);
		fw.writeLong(downPayLoad);
	}

	// 反序列化,将对象从字节输入流当中读取出来，并且序列化与反序列化的字段顺序要相同
	@Override
	public void readFields(DataInput fr) throws IOException {
		this.upPayLoad = fr.readLong();// 将上行流量给反序列化出来
		this.downPayLoad = fr.readLong(); // 将下行流量给反序列化出来
	}

	@Override
	public String toString() {
		return "" + this.upPayLoad + "\t" + this.downPayLoad + "\t" + this.loadSum;
	}
}