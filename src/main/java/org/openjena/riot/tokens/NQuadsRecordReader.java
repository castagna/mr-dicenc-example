package org.openjena.riot.tokens;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.openjena.atlas.io.PeekReader;
import org.openjena.riot.Lang;
import org.openjena.riot.lang.LangNQuads;
import org.openjena.riot.system.RiotLib;

import com.hp.hpl.jena.sparql.core.Quad;

public class NQuadsRecordReader extends RecordReader<LongWritable, Text> {
	
	  private static final byte CR = '\r';
	  private static final byte LF = '\n';
	
	private static final Log LOG = LogFactory.getLog(NQuadsRecordReader.class);

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	// private LineReader in;
	private PeekReader peekReader;
	private LangNQuads parser;
	
	private int maxLineLength;
	private LongWritable key = null;
	private Text value = null;

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		boolean skipFirstLine = false;
		if (codec != null) {
	        peekReader = PeekReader.makeUTF8(codec.createInputStream(fileIn)) ;
	        Tokenizer tokenizer = new TokenizerText(peekReader) ;
	        parser = new LangNQuads(tokenizer, RiotLib.profile(Lang.NQUADS, null), null) ;
			// in = new LineReader(codec.createInputStream(fileIn), job);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
	        peekReader = PeekReader.makeUTF8(fileIn) ;
	        Tokenizer tokenizer = new TokenizerText(peekReader) ;
	        parser = new LangNQuads(tokenizer, RiotLib.profile(Lang.NQUADS, null), null) ;
			// in = new LineReader(fileIn, job);
		}
		if (skipFirstLine) { // skip first line and re-establish "start".
			// start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
			while ( peekReader.read() != LF ) {
				start += 1;
			}
		}
		this.pos = start;
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}
		long newSize = 0;

		while ( (pos < end) && (parser.hasNext()) ) {
			newSize = (peekReader.getPosition() - peekReader.getColNum() + 1);
			Quad quad = parser.next();
			pos += newSize;
			value.set(quad.toString());

			return true;
		}

		return false;
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

	@Override
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (parser != null) {
			// parser.
		}
	}
}
