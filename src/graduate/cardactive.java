package graduate;

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

public class cardactive {
	private static class cardmaxMapper extends  Mapper<LongWritable,Text,Text,IntWritable>
	{
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] strs=value.toString().split(",");
		context.write(new Text(strs[0]),new IntWritable(1));
	}
	}

	private static class cardmaxReduce extends  Reducer<Text,IntWritable,String,IntWritable>
	{
		@Override
		protected void reduce(Text value, Iterable<IntWritable> datas,
				Reducer<Text, IntWritable, String, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		        int count=0;
		         for(IntWritable data: datas){
		             count+=data.get();
		        	 //if(data.get()>max){
		            	// max=data.get();
		             //}
		         }
		         System.out.println(value+":"+count);
		        	 context.write(value+",", new IntWritable(count));
		     }

	}
	


	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("cardmax");
	    job.setJarByClass(cardmax.class);
	    job.setMapperClass(cardmaxMapper.class);
	    job.setMapOutputKeyClass(Text.class);        
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setReducerClass(cardmaxReduce.class);
	    job.setOutputKeyClass(String.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/input/test/card_test.txt"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output/test/cardactive"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
		
	}
}
