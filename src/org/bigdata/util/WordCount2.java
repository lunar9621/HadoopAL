package org.bigdata.util;
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
/*对kmeans结果进行统计*/
public class WordCount2 {
	private static class WordCountMapper extends  Mapper<LongWritable,Text,Text,IntWritable>
	{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
	String[] strs=value.toString().split("	");

	context.write(new Text(strs[0]),new IntWritable(1));	
	}
	}

	private static class WordCountReduce extends  Reducer<Text,IntWritable,String,String>
	{
		@Override
		protected void reduce(Text value, Iterable<IntWritable> datas,
				Reducer<Text, IntWritable, String, String>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		         int count= 0;
		         for(IntWritable data: datas){
		             count +=data.get();
		         }
		         //int len = value.toString().length();
		        
		        	 System.out.println("-->"+value);
		        	 context.write("{name:'" +value+"' , value:", new IntWritable(count)+"},");
		         
		     }

	}
	
	


	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("Word Count");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(WordCountMapper.class);
	    job.setMapOutputKeyClass(Text.class);        
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setReducerClass(WordCountReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/outputkmeansresult/part-r-00000"));  
	    FileOutputFormat.setOutputPath(job, new Path("/countkmeansresult/"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
		
	}
}