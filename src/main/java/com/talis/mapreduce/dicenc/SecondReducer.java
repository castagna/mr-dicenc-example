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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SecondReducer extends Reducer<Text, Text, Text, Text> {
	
	private long counter = 0; 
	private MultipleOutputs<Text, NullWritable> mos;

	@Override
	@SuppressWarnings("unchecked")
	public void setup(Context context) {
		mos = new MultipleOutputs(context);
		
		String mapred_task_id = context.getConfiguration().get("mapred.task.id");
		counter = (Long.valueOf(mapred_task_id.substring(mapred_task_id.indexOf("_r_") + 3).replaceAll("_", "")) + 1) << 32;
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text _id = new Text("" + counter++);
		mos.write("dict", _id, key);
		
		for (Text value : values) {
			context.write(_id, value);
		}
		
	}

	@Override
	public void cleanup(Context context) throws IOException {
		try {
			mos.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
