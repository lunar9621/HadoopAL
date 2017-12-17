package graduate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigdata.HadoopCfg;


public class cardmin {
	private static class cardminMapper extends  Mapper<LongWritable,Text,Text,FloatWritable>
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

	private static class cardminReduce extends  Reducer<Text,FloatWritable,String,FloatWritable>
	{
		@Override
		protected void reduce(Text value, Iterable<FloatWritable> datas,
				Reducer<Text, FloatWritable, String, FloatWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		         Float min=111111.0f;
		         for(FloatWritable data: datas){
		             if(data.get()<min){
		            	 min=data.get();
		             }
		        	 
		         }
		         if (min<0) {
					min=0f;
				}
		         System.out.println(value+":"+min);
		        	 context.write(value+",", new FloatWritable(min));
		     }

	}
	


	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("cardmin");
	    job.setJarByClass(cardmin.class);
	    job.setMapperClass(cardminMapper.class);
	    job.setMapOutputKeyClass(Text.class);        
	    job.setMapOutputValueClass(FloatWritable.class);
	    job.setReducerClass(cardminReduce.class);
	    job.setOutputKeyClass(String.class);
	    job.setOutputValueClass(FloatWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/input/test/cardID+cost+have"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output/test/cardmincost"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
		
	}
}
