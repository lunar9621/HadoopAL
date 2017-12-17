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
/*分别统计一级分类的价格*/
public class WordCount {
	private static class WordCountMapper extends  Mapper<LongWritable,Text,Text,IntWritable>
	{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
	String[] strs=value.toString().split(",");
	if(strs[2].equals("休闲娱乐"))
	{//分区间统计
	if(Float.parseFloat(strs[0])<10)
	{strs[0]="10";}
	else if(Float.parseFloat(strs[0])<100)
		{strs[0]="100";}	
	else if(Float.parseFloat(strs[0])<500)
	{strs[0]="500";}
	else if(Float.parseFloat(strs[0])<1000)
	{strs[0]="1000";}
	else if(Float.parseFloat(strs[0])<3000)
	{strs[0]="3000";}
	else {strs[0]=">3000";}
context.write(new Text(strs[0]),new IntWritable(1));
	}
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
		         int len = value.toString().length();
		         if(len >= 2){
		        	 System.out.println("-->"+value.toString().substring(0,len - 1));
		        	 context.write("{name:'" +value.toString().substring(0,len - 1)+"' , value:", new IntWritable(count)+"},");
		         }
		     }

	}
	
	


	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("WordCount");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(WordCountMapper.class);
	    job.setMapOutputKeyClass(Text.class);        
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setReducerClass(WordCountReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/inputkmeans/"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output价格/"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
		
	}
}
