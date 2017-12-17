package org.day05.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import bigdata.HadoopCfg;

public class sort {
	private static class SortMapper extends 
	Mapper<LongWritable,Text,DoubleWritable,Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DoubleWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String[] strs = value.toString().split("\t");
		if(strs[1].contains("E")){
			
		}else{
			context.write(new DoubleWritable(Double.parseDouble(strs[1].trim())),new Text(strs[0]));
		}

	}
}

private static class SortReducer extends 
	Reducer<DoubleWritable,Text,DoubleWritable,Text>{

	@Override
	protected void reduce(DoubleWritable key, Iterable<Text> values,
			Reducer<DoubleWritable, Text, DoubleWritable, Text>.Context context) throws IOException, InterruptedException {
		for(Text value : values){
			context.write(key, value);
		}
	}
	
}

public static void main(String[] args) throws Exception{

	Configuration cfg = HadoopCfg.getCfg();

	//排序
	Job job = Job.getInstance(cfg);
	job.setJobName("Sort");
	job.setJarByClass(MostFans.class);
	job.setMapperClass(SortMapper.class);
	job.setMapOutputKeyClass(DoubleWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setReducerClass(SortReducer.class);
	job.setOutputKeyClass(DoubleWritable.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job,new Path("/input2/"));
	FileOutputFormat.setOutputPath(job,new Path("/output/"));
	System.exit( job.waitForCompletion(true)?0:1);
	}
}
