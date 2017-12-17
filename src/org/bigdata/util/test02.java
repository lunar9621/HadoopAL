package org.bigdata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;

import org.apache.hadoop.io.Text;

import bigdata.HadoopCfg;

public class test02 {
	public static void main(String[] args) throws Exception {
		Configuration cfg = HadoopCfg.getCfg();
		Path path = new Path("seq");
		Option optPath = SequenceFile.Writer.file(path);
		Option optKey = SequenceFile.Writer.keyClass(IntWritable.class);
		Option optVal = SequenceFile.Writer.valueClass(Text.class);
		Writer writer = SequenceFile.createWriter(cfg, optPath, optKey, optVal);
		for (int i = 0; i < 100; i++) {
			writer.append(new IntWritable(i), new Text("hell" + i));
		}
		writer.close();
	}
}