package graduate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigdata.HadoopCfg;

public class studentsumcost {
	private static class sumcostMapper extends  Mapper<LongWritable,Text,Text,FloatWritable>
	{
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {
	String[] strs=value.toString().split(" ");
	int len=strs[1].length();
	System.out.println(len);
   Float cost=Float.parseFloat(strs[1].substring(1, len-1));
   //Float have=Float.parseFloat(strs[2].replace("\"",""));
   System.out.println(strs[0]+":"+cost);
context.write(new Text(strs[0]),new FloatWritable(cost));
	}
	}

	private static class sumcostReduce extends  Reducer<Text,FloatWritable,String,FloatWritable>
	{
		@Override
		protected void reduce(Text value, Iterable<FloatWritable> datas,
				Reducer<Text, FloatWritable, String, FloatWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		         Float count=0.0f;
		         for(FloatWritable data: datas){
		             count +=data.get();
		        	 
		         }
		         System.out.println(value+":"+count);
		        	 context.write(value+",", new FloatWritable(count));
		     }

	}
	


	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("sumcost");
	    job.setJarByClass(studentsumcost.class);
	    job.setMapperClass(sumcostMapper.class);
	    job.setMapOutputKeyClass(Text.class);        
	    job.setMapOutputValueClass(FloatWritable.class);
	    job.setReducerClass(sumcostReduce.class);
	    job.setOutputKeyClass(String.class);
	    job.setOutputValueClass(FloatWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/input/cardId+Cost+Have"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output/sumcost"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
		
	}
}
