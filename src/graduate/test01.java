package graduate;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import bigdata.HadoopCfg;


public class test01 {
	
	/*对二级分类去重后统计二级分类数量*/

		
	public static class Map extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		protected void map(LongWritable key, Text value,
		Mapper<LongWritable, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
			System.out.println(value);
		context.write(value,new Text(""));
		}
		}

	public static class Reduce extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> arg1,
		Reducer<Text, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
		context.write(key, new Text(""));
		}

		}
		
	public static void main(String[] args) throws Exception {

		Configuration cfg = HadoopCfg.getCfg();
        Job job = Job.getInstance(cfg,"dup");
        job.setJarByClass(test01.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/input/"));  
        FileOutputFormat.setOutputPath(job, new Path("/output/test01、"));  
        System.exit(job.waitForCompletion(true)?0:1);  
}
}

