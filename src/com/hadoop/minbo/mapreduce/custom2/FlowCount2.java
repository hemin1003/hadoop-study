package com.hadoop.minbo.mapreduce.custom2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
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
 * 方式二
 */
public class FlowCount2 {

	static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// 拿到日志中的一行数据
			String line = value.toString();
			// 切分各个字段
			String[] splited = line.split(" ");
			// 获取我们所需要的字段:手机号、上行流量、下行流量
			String num = splited[0];
			String upPayLoad = splited[1];
			String downPayLoad = splited[2];
			String str = "" + upPayLoad + " " + downPayLoad;// 这样改变即可
			// 将数据进行输出
			context.write(new Text(num), new Text(str));
		}
	}

	static class WordCountReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			long payLoadSum = 0L; // 计算每个用户的上行流量和
			long downLoadSum = 0L; // 统计每个用户的下行流量和
			long sum = 0L;
			for (Text v : values) {
				String[] splited = v.toString().split(" ");
				payLoadSum += Long.parseLong(splited[0]);
				downLoadSum += Long.parseLong(splited[1]);
			}

			sum = payLoadSum + downLoadSum;
			String result = "" + payLoadSum + " " + downLoadSum + " " + sum;
			context.write(key, new Text(result));
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
		job.setJarByClass(FlowCount2.class);

		FileInputFormat.setInputPaths(job, new Path(path1));
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		// 设置reduce执行任务数，默认1个
		job.setNumReduceTasks(1);
		// 设置分区方式，默认hash
		job.setPartitionerClass(HashPartitioner.class);

		// 指定maptask的输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// 指定reducetask的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(path2));

		job.waitForCompletion(true);

		// 查看运行结果：
		FSDataInputStream fr = fileSystem.open(new Path("output1/part-r-00000"));
		IOUtils.copyBytes(fr, System.out, 2048, true);
	}
}
