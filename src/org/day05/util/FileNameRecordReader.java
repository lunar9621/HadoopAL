package org.day05.util;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileNameRecordReader extends RecordReader <Text,Text>{

	LineRecordReader lr = new LineRecordReader();
	String fileName;
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		lr.initialize(split, context);
		fileName = ((FileSplit) split).getPath().getName();    
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lr.nextKeyValue(); 
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new Text(fileName);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lr.getCurrentValue();  
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lr.getProgress(); 
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		lr.close(); 
	}

}
