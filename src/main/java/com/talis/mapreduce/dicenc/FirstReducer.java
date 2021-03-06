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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<Text,IntWritable,Text,LongWritable>  
{
	
	private static int threshold;
	private long counter = 0;
	private LongWritable v = new LongWritable();
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		threshold = context.getConfiguration().getInt("talis.dicenc.sampling.threshold", 0);
		String mapred_task_id = context.getConfiguration().get("mapred.task.id");
		counter = (Long.valueOf(mapred_task_id.substring(mapred_task_id.indexOf("_r_") + 3).replaceAll("_", ""))) << 32;
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException ,InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		
		if ( sum > threshold ) {
			v.set(counter++);	
			context.write(key, v);
		}
	}

}