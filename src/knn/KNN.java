package knn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import bigdata.HadoopCfg;

public class KNN {

	private static class KNNMapper extends 
		Mapper<LongWritable,Text,DoubleWritable,Text>{
		private static List<Point> trains =new ArrayList<>();
		@Override
		protected void setup(Mapper<LongWritable, Text, DoubleWritable, Text>.Context context)
				throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("/input/final/训练集（抽样）.csv"))));
			String line ="";
			while( (line = br.readLine()) !=null ){
				Point p = new Point(line);
				trains.add(p);
			}
		
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DoubleWritable, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit fs = (FileSplit) context.getInputSplit();
			if( fs.getPath().getName().equals("test（select）.csv") ){//测试集
				Point p1 = new Point(value.toString());//属性值加入list
				for(Point p2 : trains){
					double distance = KNNUtils.getDistance(p1, p2);
					context.write(new DoubleWritable(p1.getType()),new Text(p2.getType()+":"+distance));
					System.out.print(p1.getType());
				}
			}
		}
	}
	
	private static class KNNReducer extends 
		Reducer<DoubleWritable, Text, DoubleWritable,DoubleWritable>{
		
		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values, Reducer<DoubleWritable, Text, DoubleWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			List<KNNBean> datas = new ArrayList<>();
			for(Text value : values){
				KNNBean bean = new KNNBean(value.toString());
				datas.add(bean);
			}
			Collections.sort(datas);
		    Map<Double,Integer> map = new HashMap<>();
		    map.clear();
			for(int i = 0 ; i<100; i++){  //统计某个TYPE的元素个数
				KNNBean bean = datas.get(i);
				double type = bean.getType();
				if( map.get(type) ==null ){
					map.put(type, 1);
				}else{
					map.put(type,map.get(type)+1);
				}
			}
			double finalType = 0;
			int count = 0;
			for(Double value : map.keySet()){
				if(map.get(value) > count){
					count = map.get(value);
					finalType=value;
				}
			}
			System.out.println(key);
			context.write(key,new DoubleWritable(finalType));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration cfg = HadoopCfg.getCfg();
			Job job = Job.getInstance(cfg);
			job.setJobName("KNN");
			job.setJarByClass(KNN.class);
			job.setMapperClass(KNNMapper.class);
			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(KNNReducer.class);
			job.setOutputKeyClass(DoubleWritable.class);
			job.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(job,new Path("/input/final"));
			FileOutputFormat.setOutputPath(job,new Path("/output/sampletest"));
			job.waitForCompletion(true);
	}
}
