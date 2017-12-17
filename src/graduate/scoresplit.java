package graduate;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import bigdata.HadoopCfg;
//汇总成绩的（排名/最大排名）后，再将训练集测试集分开
public class scoresplit {
	private static class splitMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs=value.toString().split(",");
			System.out.println("length:"+strs.length);
			if(strs.length==4){
				context.write(new Text(strs[0]+","), new Text(strs[1]+","+strs[2]+","+strs[3]));
			}
			else{
				context.write(new Text(strs[0]+","), new Text(strs[1]+","+strs[2]));
			}
		}
	}

	private static class splitReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			List<Text> first=new LinkedList<Text>();
 			for(Text data:values){
				first.add(new Text(data));
			}
 			if (first.size()==2) {
			for(Text record:first){
				String[] strs=record.toString().split(",");
				if(strs.length==3)
				{
					context.write(key, new Text(strs[0]+","+strs[1]+","+strs[2]));
				}
			}	
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration cfg = HadoopCfg.getCfg();
		Job job = Job.getInstance(cfg);
		job.setJarByClass(scoresplit.class);
		job.setMapperClass(splitMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(splitReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/input/test/scoresplit"));
		FileOutputFormat.setOutputPath(job, new Path("/output/test/scoresplit"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
