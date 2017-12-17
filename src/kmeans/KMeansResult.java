package kmeans;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import bigdata.HadoopCfg;

public class KMeansResult {

	private static class KMeansMapper extends 
	Mapper<LongWritable,Text,Text,Text>{
	private Map<String,Vector<Double>> centers = new HashMap<>();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
			FileSystem fs =  FileSystem.get(context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("/output21/part-r-00000"))));
			String line = "";
			while( (line = br.readLine()) !=null ){
				String[] strs = line.split(":");
				centers.put(strs[0],Utils.str2Vector(strs[1]));
				System.out.println("--->"+Utils.str2Vector(strs[1]));
			}
			br.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		FileSplit fs = (FileSplit) context.getInputSplit();
		if(fs.getPath().getName().equals("kmeans2.csv")){
			Vector<Double> p = Utils.str2Vector2(value.toString());
			String center ="";
			double distance = Double.MAX_VALUE;
			Set<String> keys = centers.keySet();
			for(String cKey : keys){
				double temp = Utils.getDistance(p,centers.get(cKey));
				if( temp < distance){
					distance = temp;
					center = cKey;
				}
			}
			context.write(new Text(center),value);
			System.out.println("--->"+center+"-------"+"value");
		}
	}
}

private static class KMeansReducer extends 
	Reducer<Text,Text,Text,Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
			for(Text value:values){
				context.write(key, value);
			}
	}
}

public static void main(String[] args) throws Exception{
	Configuration cfg = HadoopCfg.getCfg();
		Job job = Job.getInstance(cfg);
		job.setJobName("KMeansResult");
		job.setJarByClass(KMeansResult.class);
		job.setMapperClass(KMeansMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(KMeansReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path("/input/input2"));
		FileOutputFormat.setOutputPath(job,new Path("/outputkmeansresult/"));
		job.waitForCompletion(true);

	}}