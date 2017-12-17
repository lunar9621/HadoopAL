package graduate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigdata.HadoopCfg;

public class dormselect {
	private static class dormselectMapper extends  Mapper<LongWritable,Text,Text,Text>
	{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
	String[] strs=value.toString().split(",");
	if(strs[2].equals("\"1\"")){
		context.write(new Text(strs[0]),new Text(strs[1]));
	}
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
	    job.setJobName("dormselect");
	    job.setJarByClass(dormselect.class);
	    job.setMapperClass(dormselectMapper.class);
	    job.setMapOutputKeyClass(Text.class);        
	    job.setMapOutputValueClass(Text.class);
	    //job.setReducerClass(selectoutReduce.class);
	    job.setOutputKeyClass(String.class);
	    job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/input/test/dorm_test.txt"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output/test/outdorm_test"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
		
	}

}
