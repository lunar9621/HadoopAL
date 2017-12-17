package graduate;

import java.io.IOException;
import java.math.BigDecimal;
//import .math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

//分数在学院内排名占比
public class scoratio {
	private static class scoratioMapper extends  Mapper<LongWritable,Text,IntWritable,Text>
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
	private static class scoratioReduce extends  Reducer<IntWritable,Text,String,String>//reduce函数中iteratable借口只能遍历一次
	{
		@Override
		protected void reduce(IntWritable value, Iterable<Text> datas,
				Reducer<IntWritable, Text, String, String>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<Text> first=new LinkedList<Text>();//reduce函数的iteratable只能单向遍历一次
			List<Text> second=new LinkedList<Text>();
			first.clear();
			second.clear();
			for(Text data:datas){
				first.add(new Text(data));
				second.add(new Text(data));
			}
			float max=0.0f;
			for(Text sum:first){
				String[] strs=sum.toString().split(",");
				float score=Float.parseFloat(strs[1]);
				if (score>max) {
					max=score;
				}
			}
				System.out.println(max);
			
		         for(Text record: second){
		             String[] strs=record.toString().split(",");
		             float score=Float.parseFloat(strs[1]);
		             float temp=score/max;//保留六位小数
		             BigDecimal   bg =   new  BigDecimal(temp);  
		             BigDecimal   ratio =  bg.setScale(7,  BigDecimal.ROUND_HALF_UP); 
		             System.out.println(strs[0]+"比值:"+(float)score/max+"排名:"+ratio);
		             context.write(strs[0]+","+value+","+(int)score+",", ratio.toPlainString());//ID+学院+比值(当前排名除学院最后排名)
		         }
		         
		     }

	}
	
	
	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("scoratio");
	    job.setJarByClass(scoratio.class);
	    job.setMapperClass(scoratioMapper.class);
	    job.setMapOutputKeyClass(IntWritable.class);        
	    job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(scoratioReduce.class);
	    job.setOutputKeyClass(String.class);
	    job.setOutputValueClass(FloatWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/input/score_train+test.txt"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output/scoreratio"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
		
	}
}
