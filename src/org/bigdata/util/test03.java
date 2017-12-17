package org.bigdata.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;

import bigdata.HadoopCfg;

public class test03 {
	public static void main(String[] args) throws IOException
	{
	Configuration cfg =HadoopCfg.getCfg();
	Path path = new Path("seq");
	Option optionFile  = SequenceFile.Reader.file(path);
	SequenceFile.Reader reader = new SequenceFile.Reader(cfg,optionFile);
	IntWritable key = new IntWritable();
	Text value  = new Text();
	while(reader.next(key,value)){
	System.out.println(key +" ---------> "+value);
	}
	reader.close();
	}
}
