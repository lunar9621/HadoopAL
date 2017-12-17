package kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import bigdata.HadoopCfg;

//计算种子点
public class Center {

	private static class ClusterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(",");
			for (int i = 1; i <= strs.length; i++) {
				context.write(new Text("c" + i), new IntWritable(Integer.parseInt(strs[i-1])));
			}
		}
	}

	private static  class ClusterCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {
			int max = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				if (value.get() > max) {
					max = value.get();
				}
			}
			context.write(key, new IntWritable(max));
		}
	}

	private static  class ClusterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {
			int max = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				if (value.get() > max) {
					max = value.get();
				}
			}
			context.write(key, new IntWritable(max));
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration cfg = HadoopCfg.getCfg();
		Job job = Job.getInstance(cfg);
		job.setJobName("Center");
		job.setJarByClass(Center.class);
		job.setMapperClass(ClusterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(ClusterCombiner.class);
		job.setReducerClass(ClusterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path("/input2/"));
		FileOutputFormat.setOutputPath(job,new Path("/output0/"));
		System.exit( job.waitForCompletion(true)?0:1);
	}
	
}
