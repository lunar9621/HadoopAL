
package graduate;
//将各个属性值合并到一张表上
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigdata.HadoopCfg;


public class combine2 {
	private static class combineMapper extends  Mapper<LongWritable,Text,Text,Text>
	{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
	FileSplit fs = (FileSplit) context.getInputSplit();
	String name=fs.getPath().getName();
	String[] strs=value.toString().split(",");
	if (strs.length==10) {
		context.write(new Text(strs[0]),new Text(",card,"+strs[1]+","+strs[4]+","+strs[5]+","+strs[6]+","+strs[9]));
	}
	else if( name.equals("ID+排名+排名比值（test）.csv") ){
		context.write(new Text(strs[0]),new Text(",score,"+strs[1]+","+strs[2]));
	}
else if( name.equals("每人去图书馆次数.csv") ){
	context.write(new Text(strs[0]),new Text(",library,"+strs[1]));
	}
else if( name.equals("晚归寝室次数统计.csv") ){
	context.write(new Text(strs[0]),new Text(",backdorm,"+strs[1]));
}
else {
	context.write(new Text(value),new Text(",subsidy,"+value));
}
	}
	}

	private static class combineReduce extends  Reducer<Text,Text,FloatWritable,Text>
	{
		@Override
		protected void reduce(Text value, Iterable<Text> datas,
				Reducer<Text, Text, FloatWritable, Text>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Float[] strs=new Float[10];
			for(int i=0;i<10;i++){
				strs[i]=0f;
			}
		         for(Text data: datas){
		        	 String[] temp=data.toString().split(",");
		        	 System.out.println(temp[1]+":"+temp[2]);
		        	 if(temp[1].equals("card")){
		        		 System.out.println(temp[3]);
		        		 strs[0]=Float.parseFloat(temp[2])/100;
		        		 strs[1]=Float.parseFloat(temp[3])/1000;
		        		 strs[2]=Float.parseFloat(temp[4]);
		        		 strs[3]=Float.parseFloat(temp[5]);
		        		 strs[4]=Float.parseFloat(temp[6])/100;
		        	 }
		        	 else if(temp[1].equals("score")){
		        		 strs[5]=Float.parseFloat(temp[2])/10;
		        		 strs[6]=Float.parseFloat(temp[3])*100;
		        	 }
		        	 else if(temp[1].equals("library")){
		        		 strs[7]=Float.parseFloat(temp[2])/10;
		        	 }
		        	 else if(temp[1].equals("backdorm")){
		        		 strs[8]=Float.parseFloat(temp[2]);
		        	 }

		         }
		         String id=value.toString();
		         Float idvalue=Float.parseFloat(id)/10000;
		         context.write(new FloatWritable(idvalue), new Text(","+strs[0]+","+strs[1]+","+strs[2]+","+strs[3]+","+strs[4]+","+strs[5]+","+strs[6]+","+strs[7]+","+strs[8]));
		     }

	}
	public static void main(String[] args) throws Exception
	{
		Configuration cfg =HadoopCfg.getCfg();
	    Job job = Job.getInstance(cfg);
	    job.setJobName("combine2");
	    job.setJarByClass(combine2.class);
	    job.setMapperClass(combineMapper.class);
	    job.setMapOutputKeyClass(Text.class);        
	    job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(combineReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path("/input/test/combinetest"));  
	    FileOutputFormat.setOutputPath(job, new Path("/output/test/combinetest"));  
	    System.exit(job.waitForCompletion(true)?0:1);  
	}
	
}
