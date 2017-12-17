package graduate;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import bigdata.HadoopCfg;

public class cardratio {
	private static class cardratioMapper extends  Mapper<LongWritable,Text,Text,Text>
	{
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] strs=value.toString().split(",");
		System.out.println(strs[1]+":"+strs[2]+":"+strs[3]+":"+strs[4]+":"+strs[5]);
	  BigDecimal haveratio,maxratio,minratio,sumratio;
	   //int i,j;
	   Float[] a=new Float[5];
	   for(int i=0;i<5;i++){
			a[i]=0f;
		}
	   //strs[0]=Float.parseFloat(temp[2])/100;
	   a[0]=Float.parseFloat(strs[1]);
	   a[1]=Float.parseFloat(strs[2]);
	   a[2]=Float.parseFloat(strs[3]);
	   a[3]=Float.parseFloat(strs[4]);
	   a[4]=Float.parseFloat(strs[5]);//活跃天数
	   BigDecimal   bg1=new  BigDecimal(a[0]/a[4]);  
       haveratio =  bg1.setScale(7,  BigDecimal.ROUND_HALF_UP); 
       BigDecimal   bg2=new  BigDecimal(a[1]/a[4]);  
       maxratio =  bg2.setScale(7,  BigDecimal.ROUND_HALF_UP); 
       BigDecimal   bg3=new  BigDecimal(a[2]/a[4]);  
       minratio =  bg3.setScale(7,  BigDecimal.ROUND_HALF_UP); 
       BigDecimal   bg4=new  BigDecimal(a[3]/a[4]);  
       sumratio =  bg4.setScale(7,  BigDecimal.ROUND_HALF_UP); 
	  context.write(new Text(strs[0]),new Text(","+strs[1]+","+strs[2]+","+strs[3]+","+strs[4]+","+strs[5]+","+haveratio+","+maxratio+","+minratio+","+sumratio));
	}
	}

	/*private static class cardratioReduce extends  Reducer<Text,Text,String,IntWritable>
	{
		@Override
		protected void reduce(Text value, Iterable<Text> datas,
				Reducer<Text, Text, String, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
for(data:datas)
		}  
	}*/
	


	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("cardratio");
	    job.setJarByClass(cardratio.class);
	    job.setMapperClass(cardratioMapper.class);
	    job.setMapOutputKeyClass(Text.class);        
	    job.setMapOutputValueClass(Text.class);
	    //job.setReducerClass(cardratioReduce.class);
	    job.setOutputKeyClass(String.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/input/test/余额最大+单笔最大+单笔最小+花费总额 +活跃次数+比例.csv"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output/test/cardratio"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
		
	}
}

