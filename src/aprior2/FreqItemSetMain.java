package aprior2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import bigdata.HadoopCfg;

public class FreqItemSetMain
{
	public static void main(String[] args) throws Exception
	{
		Configuration conf = HadoopCfg.getCfg();
		FileSystem fs = FileSystem.get(conf);
		Path path_in, path_out;
		conf.set("support","2");
		conf.set("cur_k","2");
		{
		Job job = Job.getInstance(conf);
		job.setJarByClass(FreqItemSetMain.class);
		job.setMapperClass(FreqItemSetRun.MiningMapper.class);
		job.setCombinerClass(FreqItemSetRun.MiningCombiner.class);
		job.setReducerClass(FreqItemSetRun.MiningReducer.class);
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.setNumLinesPerSplit(job, 1);
		job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
		
		path_in = new Path("/FreqInput");
		NLineInputFormat.setInputPaths(job, path_in);
		path_out = new Path("/Freqoutput");
		if(fs.exists(path_out))
    		fs.delete(path_out, true);
		FileOutputFormat.setOutputPath(job, path_out);
		if(job.waitForCompletion(true)==false)
			System.exit(1);
		}
			
    	
    	fs.close();
    	System.exit(0);
	}
}