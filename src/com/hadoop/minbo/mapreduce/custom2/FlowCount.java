package com.hadoop.minbo.mapreduce.custom2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 方式一
 */
public class FlowCount {

	static class WordCountMapper extends Mapper<Object, Text, Text, MyData> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// 拿到日志中的一行数据
			String line = value.toString();
			// 切分各个字段
			String[] splited = line.split(" ");
			// 获取我们所需要的字段:手机号、上行流量、下行流量
			String msisdn = splited[0];
			long upPayLoad = Long.parseLong(splited[1]);
			long downPayLoad = Long.parseLong(splited[2]);
			// 将数据进行输出
			context.write(new Text(msisdn), new MyData(upPayLoad, downPayLoad));
		}
	}

	static class WordCountReducer extends Reducer<Text, MyData, Text, MyData> {
		@Override
		protected void reduce(Text key, Iterable<MyData> values, Reducer<Text, MyData, Text, MyData>.Context context)
				throws IOException, InterruptedException {
			long payLoadSum = 0L; // 计算每个用户的上行流量和
			long downLoadSum = 0L; // 统计每个用户的下行流量和
			// 数据传递过来的时候：<手机号，{MyData1, MyData2, MyData3……}>
			for (MyData md : values) {
				payLoadSum += md.upPayLoad;
				downLoadSum += md.downPayLoad;
			}
			// 在此需要重写toString()方法
			context.write(key, new MyData(payLoadSum, downLoadSum)); 
		}
	}

	public static String path1 = "input1";
	public static String path2 = "output1";

	public static void main(String[] args) throws Exception {
		// Window下运行设置
		System.setProperty("hadoop.home.dir", "F:\\hadoop\\hadoop-2.7.3"); // 设置hadoop安装路径
		System.setProperty("HADOOP_USER_NAME", "hadoop"); // 用户名

		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(path2))) {
			fileSystem.delete(new Path(path2), true);
		}

		Job job = Job.getInstance(conf);
		job.setJarByClass(FlowCount.class);

		FileInputFormat.setInputPaths(job, new Path(path1));
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		//设置reduce执行任务数，默认1个
		job.setNumReduceTasks(1);
		//设置分区方式，默认hash
		job.setPartitionerClass(HashPartitioner.class);

		// 指定maptask的输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MyData.class);

		// 指定reducetask的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MyData.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(path2));

		job.waitForCompletion(true);

		// 查看运行结果：
		FSDataInputStream fr = fileSystem.open(new Path("output1/part-r-00000"));
		IOUtils.copyBytes(fr, System.out, 2048, true);
	}
}
