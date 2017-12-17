package graduate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigdata.HadoopCfg;
//判断学生 进入图书馆次数
public class libraryselect {
	private static class libraryselectMapper extends  Mapper<LongWritable,Text,Text,IntWritable>
	{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
	String[] strs=value.toString().split(",");
	char a=strs[1].charAt(1);
	if (strs[1].length()>4&&a=='进') {
		System.out.println(strs[0]+":"+strs[1]+":"+strs[2]);
		context.write(new Text(strs[0]),new IntWritable(1));	
	}
	}
	}
	private static class libraryselectReduce extends  Reducer<Text,IntWritable,String,IntWritable>
	{
		@Override
		protected void reduce(Text value, Iterable<IntWritable> datas,
				Reducer<Text, IntWritable, String, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		         int count= 0;
		         for(IntWritable data: datas){
		             count +=data.get();
		         }
		        	 System.out.println(value+":"+count);
		        	 context.write(value+",", new IntWritable(count));
		    
		     }

	}
	
	
	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("libraryselect");
	    job.setJarByClass(libraryselect.class);
	    job.setMapperClass(libraryselectMapper.class);
	    job.setMapOutputKeyClass(Text.class);        
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setReducerClass(libraryselectReduce.class);
	    job.setOutputKeyClass(String.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/input/test/library_test.txt"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output/test/libraryuse"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
		
	}
}
