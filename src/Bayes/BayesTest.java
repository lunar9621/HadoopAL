package Bayes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import bigdata.HadoopCfg;

/**
 * 贝叶斯预测算法
 * @author Phenix
 *
 */
public class BayesTest {
	
	private static class BayesTestMapper extends 
		Mapper<LongWritable,Text,Text,Text>{
		private Map<String,Integer> fy = new HashMap<>(); //类别频度表
		private Map<String,Integer> fxy = new HashMap<>();//属性频度表
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("/output/Bayes/train/part-r-00000"))));
			String line = "";
			while( (line = br.readLine()) !=null ){
				String[] strs  = line.split("\t");
				if(line.contains(":")){
					fxy.put(strs[0],Integer.parseInt(strs[1]));
				}else{
					fy.put(strs[0],Integer.parseInt(strs[1]));
				}
			}
			br.close();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(",");
			int max  = - 1;
			String finalType = "";
			for(String type : fy.keySet()){
				int tempmax = 1;
				int fycount = fy.get(type);
				tempmax = tempmax * fycount;
				for(int i = 0 ; i<strs.length-1 ; i++){
					Integer fxycount = fxy.get(type+":"+i+":"+strs[i]);
					if(fxycount !=null ){
						tempmax = tempmax * (fxycount+1);
					}
				}
				if(tempmax > max){
					max = tempmax;
					finalType = type;
				}
			}
			context.write(new Text(strs[strs.length-1]), new Text(finalType));
		}
	}
	private static class BayesTestReduce extends 
		Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for(Text value:arg1){
				context.write(arg0, value);
			}

		}
		
	}
	
	public static void main(String[] args) throws Exception{
		Configuration cfg = HadoopCfg.getCfg();
		Job job = Job.getInstance(cfg);
		job.setJobName("BayesTest");
		job.setJarByClass(BayesTest.class);
		job.setMapperClass(BayesTestMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(BayesTestReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path("/input/final/test（select）.csv"));
		FileOutputFormat.setOutputPath(job,new Path("/output/Bayes/result"));
		System.exit( job.waitForCompletion(true)?0:1);
	}
}
