package Bayes;

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
 * 贝叶斯训练算法
 * 计算每个分类Yi出现的频度 P(Yi)，以及每个属性值xj出现在Yi中的频度 P(xj|Yi)
 * @author Phenix
 *
 */
public class BayesTrain {

	private static class BayesTrainMapper extends
		Mapper<LongWritable,Text, Text,IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(",");
			context.write(new Text(strs[strs.length-1]),new IntWritable(1));
			for(int i = 0 ; i < strs.length-1; i++){
				context.write(new Text(strs[strs.length-1]+":"+i+":"+strs[i]), new IntWritable(1));
			}
		}
	}
	
	private static class BayesTrainReducer extends 
		Reducer<Text,IntWritable,Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value:values){
				sum += value.get();
			}
			context.write(key,new IntWritable(sum));
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration cfg = HadoopCfg.getCfg();
		Job job = Job.getInstance(cfg);
		job.setJobName("BayesTrain");
		job.setJarByClass(BayesTrain.class);
		job.setMapperClass(BayesTrainMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(BayesTrainReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path("/input/final/训练集（抽样）.csv"));
		FileOutputFormat.setOutputPath(job,new Path("/output/Bayes/train"));
		System.exit( job.waitForCompletion(true)?0:1);
	}
	
}
