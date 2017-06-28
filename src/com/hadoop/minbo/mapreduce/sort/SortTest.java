package com.hadoop.minbo.mapreduce.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * mapreduce之自定义排序算法
 */
public class SortTest {

	static class MyMapper extends Mapper<LongWritable, Text, KeySort, LongWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(" ");
			KeySort kSort = new KeySort(Long.parseLong(split[0]), Long.parseLong(split[1]));
			context.write(kSort, new LongWritable(Long.parseLong(split[1])));
		}
	}

	static class MyReduce extends Reducer<KeySort, LongWritable, LongWritable, LongWritable> {
		@Override
		protected void reduce(KeySort kSort, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(kSort.key), new LongWritable(kSort.value));
		}
	}

	public static String path1 = "input2";
	public static String path2 = "output2";

	public static void main(String[] args) throws Exception {
		// Window下运行设置
		System.setProperty("hadoop.home.dir", "F:\\hadoop\\hadoop-2.7.3"); // 设置hadoop安装路径
		System.setProperty("HADOOP_USER_NAME", "hadoop"); // 用户名

		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(path2))) {
			fileSystem.delete(new Path(path2), true);
		}

		// 新建一个job
		Job job = Job.getInstance(conf);

		// 设置jar包所在路径
		job.setJarByClass(SortTest.class);

		// 指定mapper和reducer类
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);

		// 指定maptask的输出类型
		job.setMapOutputKeyClass(KeySort.class);
		job.setMapOutputValueClass(LongWritable.class);

		// 指定reducetask的输出类型
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		// 指定该mapreduce程序数据的输入输出路径
		Path inputPath = new Path(path1);
		Path outputPath = new Path(path2);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		// 指定分区类
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);

		// 最后提交任务，给JobTracker执行
		job.waitForCompletion(true);

		// 查看运行结果：
		FSDataInputStream fr = fileSystem.open(new Path(path2 + "/part-r-00000"));
		IOUtils.copyBytes(fr, System.out, 2048, true);
	}
}
