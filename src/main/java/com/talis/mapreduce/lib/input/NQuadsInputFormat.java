package com.talis.mapreduce.lib.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.openjena.riot.tokens.NQuadsRecordReader;

public class NQuadsInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new NQuadsRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		return codec == null;
	}

}
