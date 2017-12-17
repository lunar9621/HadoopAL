package org.day05.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * 处理微博任务关系，得到 rand.txt
 * @author Phenix
 *
 */
public class Second {
	
	private static class SecondMapper extends 
		Mapper<LongWritable,Text,Text,IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");
			context.write(new Text(strs[0]),new IntWritable());
		}
		
	}
	
	private static class SecondReducer extends 
		Reducer<Text,IntWritable,Text, DoubleWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, DoubleWritable>.Context context)
						throws IOException, InterruptedException {
			context.write(key,new DoubleWritable(100000000.0/61972));
		}
		
	}
	public static void main(String[] args) throws Exception{
		Configuration cfg = HadoopCfg.getCfg();
		Job job = Job.getInstance(cfg);
		job.setJobName("Second");
		job.setJarByClass(Second.class);
		job.setMapperClass(SecondMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(SecondReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job,new Path("/input/"));
		FileOutputFormat.setOutputPath(job,new Path("/output1"));
		System.exit( job.waitForCompletion(true)?0:1);
	}
}
