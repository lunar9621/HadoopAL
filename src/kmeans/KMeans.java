package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import bigdata.HadoopCfg;

public class KMeans {

	private static class KMeansMapper extends 
		Mapper<LongWritable,Text,Text,Text>{
		private Map<String,Vector<Double>> centers = new HashMap<>();//中心点及各个维度的值

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				FileSystem fs =  FileSystem.get(context.getConfiguration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(context.getConfiguration().get("clusterPath")))));
				String line = "";
				//读取初始中心点
				while( (line = br.readLine()) !=null ){
					String[] strs = line.split(":");
					centers.put(strs[0],Utils.str2Vector(strs[1]));
				}
				br.close();
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit fs = (FileSplit) context.getInputSplit();
			if(fs.getPath().getName().equals("kmeans.csv")){
				System.out.println("--------->"+fs.getPath().getName());
				Vector<Double> p = Utils.str2Vector(value.toString());//一个点的所有维数存储到一个list中
				String center ="";
				double distance = Double.MAX_VALUE;
				Set<String> keys = centers.keySet();
				for(String cKey : keys){
					double temp = Utils.getDistance(p,centers.get(cKey));//p：待分类的点
					if( temp < distance){
						distance = temp;
						center = cKey;
					}
				}
				context.write(new Text(center),value);
			}
		}
	}
	
	private static class KMeansReducer extends 
		Reducer<Text,Text,Text,NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			List<Vector<Double>> list = new ArrayList<>();
			int size = 0;
			for(Text value: values){
				Vector<Double> p = Utils.str2Vector(value.toString());
				size = p.size();
				list.add(p);
			}
			context.write(new Text(key.toString()+":"+Utils.avgDistance(list,size)),NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration cfg = HadoopCfg.getCfg();
		for(int i = 1 ; i <= 20 ;i++){
			cfg.set("clusterPath", "/output" + i + "/part-r-00000");//使每次读入的文件更新
			Job job = Job.getInstance(cfg);
			job.setJobName("KMeans");
			job.setJarByClass(KMeans.class);
			job.setMapperClass(KMeansMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(KMeansReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			FileInputFormat.addInputPath(job,new Path("/input/"));
			FileInputFormat.addInputPath(job,new Path("/output"+i));
			FileOutputFormat.setOutputPath(job,new Path("/output"+(i+1)));
			job.waitForCompletion(true);
		}
	}
}
