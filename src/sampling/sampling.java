package sampling;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import java.util.Collections;

import java.util.List;

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
import org.jboss.netty.handler.codec.http.websocketx.PingWebSocketFrame;

import com.google.common.base.Strings;

import bigdata.HadoopCfg;

public class sampling {
	private static class sampleMapper extends 
		Mapper<LongWritable,Text,Text,Text>{
		private static List<Point> trains =new ArrayList<>();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("/input/sample/少数类.csv"))));
			String line ="";
			while( (line = br.readLine()) !=null ){
				Point p = new Point(line);
				trains.add(p);
			}
		
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit fs = (FileSplit) context.getInputSplit();
			if( fs.getPath().getName().equals("多数类.csv") ){//测试集
				Point p1 = new Point(value.toString());//测试集  属性值加入list
				for(Point p2 : trains){
					double distance = KNNUtils.getDistance(p1, p2);
					double[] strs=new double[p1.getV().size()+1];
					for(int i=0;i<p1.getV().size();i++){
						strs[i]=p1.getV().get(i);
					}
					context.write(new Text(p1.getType()+","+strs[1]+","+strs[2]+","+strs[3]+","+strs[4]+","+strs[5]+","+strs[6]+","+strs[7]+","+strs[8]+","+strs[9]),new Text(p2.getType()+":"+distance));
					System.out.println(p1.getType());
				}
			}
		}
	}
	
	private static class sampleReducer extends 
		Reducer<Text, Text, Text,DoubleWritable>{
	
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println(key+"2");
			double dis=0.0;
			List<KNNBean> datas = new ArrayList<>();
			for(Text value : values){
				KNNBean bean = new KNNBean(value.toString());
				datas.add(bean);
			}
			Collections.sort(datas);
			for(int i=datas.size()-1;i>=datas.size()-3;i--){
				dis+=datas.get(i).getDistance();
			}
			dis/=3;//到最远的三个少数类样本平均距离;
	    context.write(new Text(key+","), new DoubleWritable(dis));
	       dis=0.0;
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration cfg = HadoopCfg.getCfg();
			Job job = Job.getInstance(cfg);
			job.setJobName("sampling");
			job.setJarByClass(sampling.class);
			job.setMapperClass(sampleMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(sampleReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(job,new Path("/input/sample"));
			FileOutputFormat.setOutputPath(job,new Path("/output/sample"));
			job.waitForCompletion(true);
	}
}
