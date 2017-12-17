package org.day05.util;

import java.io.IOException;

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

/**
 * 处理userrelation.txt(微博任务关系)，得到 links.txt
 * @author Phenix
 *
 */
public class First {
	
	private static class FirstMapper extends 
		Mapper<LongWritable,Text,Text,Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			context.write(new Text( strs[0].trim() ),new Text(strs[1].trim()));
		}
	}
	
	private static class FirstReducer extends 
		Reducer<Text,Text,Text,Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text value : values){
				sb.append(value.toString()).append("\t");
			}
			context.write(key,new Text(sb.toString()));
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		Configuration cfg = HadoopCfg.getCfg();
		Job job = Job.getInstance(cfg);
		job.setJobName("First");
		job.setJarByClass(First.class);
		job.setMapperClass(FirstMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(FirstReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path("/input/"));
		FileOutputFormat.setOutputPath(job,new Path("/output1"));
		System.exit( job.waitForCompletion(true)?0:1);
	}
	
}

