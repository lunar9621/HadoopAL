package org.bigdata.util;

import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

import bigdata.HadoopCfg;

public class test {
public static void main(String[] args) throws Exception
{
	Configuration cfg =HadoopCfg.getCfg();
	FileSystem fs=FileSystem.get(cfg);
	Path output=new Path("hello.gz");
	OutputStream os = fs.create((org.apache.hadoop.fs.Path) output);
	CompressionCodec codec = new GzipCodec();
	byte[] datas = "Hello World".getBytes();
	CompressionOutputStream cos = codec.createOutputStream(os);
	cos.write(datas);
	cos.close();
	
}
}
