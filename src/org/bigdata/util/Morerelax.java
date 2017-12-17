package org.bigdata.util;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigdata.HadoopCfg;

public class Morerelax {
	private static class MoreMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");

			if (!"child".equals(strs[0])) {
				String child = strs[0];
				String parent = strs[1];
				String type = "1"; // 左表
				context.write(new Text(parent), new Text(type + ":" + child));
				type = "2";// 右表
				context.write(new Text(child), new Text(type + ":" + parent));
			}
		}
	}

	private static class MoreReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			List<String> leftList = new LinkedList<>();
			List<String> rightList = new LinkedList<>();
			for (Text value : values) {
				String[] strs = value.toString().split(":");
				if ("1".equals(strs[0])) {
					leftList.add(strs[1]);
				} else {
					rightList.add(strs[1]);
				}
			}
			for (String str : leftList) {
				for (String str2 : rightList) {
					context.write(new Text(str), new Text(str2));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration cfg = HadoopCfg.getCfg();
		Job job = Job.getInstance(cfg);
		job.setJarByClass(Morerelax.class);
		job.setMapperClass(MoreMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MoreReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/MoreRelax/"));
		FileOutputFormat.setOutputPath(job, new Path("/output/MoreRelax/"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
