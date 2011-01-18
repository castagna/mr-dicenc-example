/*
 * Copyright © 2011 Talis Systems Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.talis.mapreduce.dicenc;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static int percentage;
	
	private Random random = new Random(System.currentTimeMillis());
	private Text k = new Text();
    private final static IntWritable one = new IntWritable(1);
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		percentage = context.getConfiguration().getInt("talis.dicenc.sampling.percentage", 10);
	}
	
	@Override
	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] t = Utils.parseTripleOrQuad(value.toString());
		for (String n : t) {
			int r = random.nextInt(100);
			if ( r < percentage ) {
				k.set(n);
				context.write(k, one);
			}
		}

	}

}
