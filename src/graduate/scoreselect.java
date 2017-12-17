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

//选择每个学院排名前50学生
public class scoreselect {
	private static class scoreselectMapper extends  Mapper<LongWritable,Text,IntWritable,Text>
	{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
	String[] strs=value.toString().split(",");
	int depart=Integer.parseInt(strs[1]);
	context.write(new IntWritable(depart),new Text(strs[0]+","+strs[2]));
	System.out.println(depart+":"+strs[0]+":"+strs[2]);
	}
	}
	private static class scoreselectReduce extends  Reducer<IntWritable,Text,String,IntWritable>
	{
		@Override
		protected void reduce(IntWritable value, Iterable<Text> datas,
				Reducer<IntWritable, Text, String, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		         for(Text data: datas){
		             String[] strs=data.toString().split(",");
		             int score=Integer.parseInt(strs[1]);
		             if (score<=50) {
		            	 context.write(strs[0]+","+value+",", new IntWritable(score));
					}
		         }
		     }

	}
	
	
	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("scoreselect");
	    job.setJarByClass(scoreselect.class);
	    job.setMapperClass(scoreselectMapper.class);
	    job.setMapOutputKeyClass(IntWritable.class);        
	    job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(scoreselectReduce.class);
	    job.setOutputKeyClass(String.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/input/score_train.txt"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output/scoreselect50"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
		
	}
}
