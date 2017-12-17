package org.bigdata.util;
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

public class Distinct {
	
	/*对二级分类去重后统计二级分类数量*/

		
		private static class DistinctMapper extends Mapper<LongWritable, Text,Text ,Text>{

			@Override
			protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				context.write(value,new Text(""));
			}
			
		}
		private static class DistinctReduce extends Reducer<Text,Text,Text,Text>{

			@Override
			protected void reduce(Text key, Iterable<Text>  value,
					Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				context.write(key, new Text(""));
			}
			
			
		}
		
		public static void main(String[] args) throws Exception
		{
			Configuration cfg =HadoopCfg.getCfg();
		    Job job = Job.getInstance(cfg);
		    job.setJobName("Distinct");
		    job.setJarByClass(Distinct.class);
		    job.setMapperClass(DistinctMapper.class);
		    job.setMapOutputKeyClass(Text.class);        
		    job.setMapOutputValueClass(Text.class);
		    job.setReducerClass(DistinctReduce.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path("/a/"));  
		    FileOutputFormat.setOutputPath(job, new Path("/output二级分类/"));  
		    System.exit(job.waitForCompletion(true)?0:1);  
			
		}

	}

