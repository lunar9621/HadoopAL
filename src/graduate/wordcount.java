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
public class wordcount {
	private static class wordcountMapper extends  Mapper<LongWritable,Text,Text,IntWritable>
	{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
	String[] strs=value.toString().split("	");
	Float num=Float.parseFloat(strs[1]);
	if(num>=23||num<=5)
	{
context.write(new Text(strs[0]),new IntWritable(1));
	}
	}
	}

	private static class wordcountReduce extends  Reducer<Text,IntWritable,Text,IntWritable>
	{
		@Override
		protected void reduce(Text value, Iterable<IntWritable> datas,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		         int count= 0;
		         for(IntWritable data: datas){
		             count +=data.get();
		         }
		        
		        	 context.write(new Text(value+","), new IntWritable(count));
		     }

	}
	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("wordcount");
	    job.setJarByClass(wordcount.class);
	    job.setMapperClass(wordcountMapper.class);
	    job.setMapOutputKeyClass(Text.class);        
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setReducerClass(wordcountReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/input/test/outdorm_hour"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output/test/outdorm.count"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
	}
	
}
