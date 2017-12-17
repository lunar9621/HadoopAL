package org.day05.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigdata.HadoopCfg;

public class PeopleRank {
	private static Map<String,Double> table = new HashMap<>();
	private static class FirstMapper extends 
		Mapper<Text, Text, Text,DoubleWritable>{
		
		
		@Override
		protected void setup(Mapper<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {		
			super.setup(context);
			Configuration cfg = context.getConfiguration();
			FileSystem fs = FileSystem.get(cfg);
			Path path = new Path(cfg.get("rand"));
			RemoteIterator<LocatedFileStatus> datas = fs.listFiles(path,false);
			while(datas.hasNext()){
				LocatedFileStatus status = datas.next();
				if( status.getLen() > 0 ){
					Path filePath = status.getPath();
					BufferedReader br = new BufferedReader( new InputStreamReader( fs.open(filePath)));
					String line ="";
					while( (line = br.readLine())!=null){
						String[] strs = line.split("\t");
						table.put(strs[0], Double.parseDouble(strs[1]));
					}
				}
			}
		}

		@Override
		protected void map(Text fileName, Text value, Mapper<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
				if(fileName.toString().equals("links.txt")){
					String[] strs = value.toString().split("\t");
					context.write(new Text(strs[0]),new DoubleWritable(0.0));
					double temp=table.get(strs[0]);
					for(int i = 1 ; i < strs.length;i++){
						context.write(new Text(strs[i]),new DoubleWritable(temp/(strs.length-1)));
					}
				}
		}
	}
	
	private static class FirstReducer extends 
		Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			double total =0.0;
			for(DoubleWritable value : values){
				total = total + value.get();
			}

			double temp=table.get(key.toString())==null ? 0.0 : table.get(key.toString());
			total = 0.85*total +0.15 * temp;
			context.write(key,new DoubleWritable(total));
		}

		@Override
		protected void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
		
	}
	public static void main(String[] args) throws Exception{
		for(int i = 1 ; i< 10;i++){
			Configuration cfg = HadoopCfg.getCfg();
			cfg.set("rand","/output"+i);
			Job job = Job.getInstance(cfg);
			job.setJobName("PageRank");
			job.setJarByClass(PeopleRank.class);
			job.setInputFormatClass(FileNameInputFormat.class);
			job.setMapperClass(FirstMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			job.setReducerClass(FirstReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(job,new Path("/input/"));
			FileInputFormat.addInputPath(job,new Path("/output"+i));
			FileOutputFormat.setOutputPath(job,new Path("/output"+(i+1)));
			job.waitForCompletion(true);
			
		}

	}
}
