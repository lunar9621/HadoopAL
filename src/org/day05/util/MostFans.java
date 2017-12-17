package org.day05.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import bigdata.HadoopCfg;

/**
 * 统计粉丝数
 * @author Phenix
 *
 */
public class MostFans {
	private static class FansMapper extends 
	Mapper<LongWritable,Text,Text,Text>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] strs = value.toString().split("\t");
		context.write(new Text( strs[1].trim() ),new Text(strs[0].trim()));
	}
}

private static class FansReducer extends 
	Reducer<Text,Text,Text,IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int total = 0;
		for(Text value : values){
			value.toString();
			total++;
		}
		context.write(key,new IntWritable(total));
	}
	
}

private static class SortMapper extends 
	Mapper<LongWritable,Text,IntWritable,Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String[] strs = value.toString().split("\t");
		context.write(new IntWritable(Integer.parseInt(strs[1].trim())),new Text(strs[0]));
	}
}

private static class SortReducer extends 
	Reducer<IntWritable,Text,IntWritable,Text>{

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
		for(Text value : values){
			context.write(key, value);
		}
	}
	
}

public static void main(String[] args) throws Exception{
	//统计
	Configuration cfg = HadoopCfg.getCfg();
	Job job = Job.getInstance(cfg);
	job.setJobName("Fans");
	job.setJarByClass(MostFans.class);
	job.setMapperClass(FansMapper.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	job.setReducerClass(FansReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job,new Path("/MostFans/userrelation.txt"));
	FileOutputFormat.setOutputPath(job,new Path("/outmostfans/"));
	job.waitForCompletion(true);
	//排序
	 job = Job.getInstance(cfg);
	job.setJobName("Sort");
	job.setJarByClass(MostFans.class);
	job.setMapperClass(SortMapper.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setReducerClass(SortReducer.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job,new Path("/outmostfans/"));
	FileOutputFormat.setOutputPath(job,new Path("/outmostfans2/"));
	System.exit( job.waitForCompletion(true)?0:1);
}

}

