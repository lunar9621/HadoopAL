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

//统计每个学院有成绩记录的学生人数
public class scoredepartcount {
	private static class scoredepartMapper extends  Mapper<LongWritable,Text,IntWritable,IntWritable>
	{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
	String[] strs=value.toString().split(",");
	int depart=Integer.parseInt(strs[1]);
	context.write(new IntWritable(depart),new IntWritable(1));
	}
	}
	private static class scoredepartReduce extends  Reducer<IntWritable,IntWritable,String,IntWritable>
	{
		@Override
		protected void reduce(IntWritable value, Iterable<IntWritable> datas,
				Reducer<IntWritable, IntWritable, String, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count=0;
		         for(IntWritable data: datas){
		          count+=data.get();  
		         }
		         System.out.println(value+":"+count);
		            	 context.write(value+",", new IntWritable(count));
					
		         }
     }

	
	
	
	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("scoredepart");
	    job.setJarByClass(scoredepartcount.class);
	    job.setMapperClass(scoredepartMapper.class);
	    job.setMapOutputKeyClass(IntWritable.class);        
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setReducerClass(scoredepartReduce.class);
	    job.setOutputKeyClass(String.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/input/score_train.txt"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output/scodepcount"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
		
	}
}
