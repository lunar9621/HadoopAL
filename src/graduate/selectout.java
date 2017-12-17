package graduate;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.util.WordCount;

import bigdata.HadoopCfg;

public class selectout {
	private static class selectoutMapper extends  Mapper<LongWritable,Text,Text,IntWritable>
	{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
	String[] strs=value.toString().split(" ");
	char a,b;
	int hour;
	a=strs[1].charAt(0);
	b=strs[1].charAt(1);
	hour=(int)(a-48)*10+b-48;
	String[] id=strs[0].split("	");
	System.out.println(value+" "+hour);
context.write(new Text(id[0]),new IntWritable(hour));
	}
	}

	/*private static class selectoutReduce extends Reducer<String,Text,String,NullWritable>{

		@Override
		protected void reduce(String key, Iterable<Text>  value,
				Reducer<String, Text, String, NullWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key,NullWritable.get());
		}
		
		
	}*/
	


	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("graduateWordCount");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(selectoutMapper.class);
	    job.setMapOutputKeyClass(Text.class);        
	    job.setMapOutputValueClass(IntWritable.class);
	    //job.setReducerClass(selectoutReduce.class);
	    job.setOutputKeyClass(String.class);
	    job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/input/test/backdorm_test"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output/test/backdormhour"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
		
	}

}
