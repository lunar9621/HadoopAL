package graduate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigdata.HadoopCfg;

public class cardselect {
	private static class cardselectMapper extends  Mapper<LongWritable,Text,Text,Text>
	{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
	String[] strs=value.toString().split(",");
context.write(new Text(strs[0]+" "+strs[5]+" "+strs[6]),new Text(""));
	}
	}

	
	
	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("graduatecardselect");
	    job.setJarByClass(cardselect.class);
	    job.setMapperClass(cardselectMapper.class);
	    job.setMapOutputKeyClass(Text.class);        
	    job.setMapOutputValueClass(Text.class);
	    //job.setReducerClass(selectoutReduce.class);
	    job.setOutputKeyClass(String.class);
	    job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/input/test/card_test.txt"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output/test/card+cost+have"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
		
	}
}
