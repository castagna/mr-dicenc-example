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

public class ThirdReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String s = null;
		String p = null;
		String o = null;
		String g = null;
		
		for (Text value : values) {
			String[] v = value.toString().split("-");
			
			if ( v[1].equals("s") ) s = v[0];
			if ( v[1].equals("p") ) p = v[0];
			if ( v[1].equals("o") ) o = v[0];
			if ( v[1].equals("g") ) g = v[0];
		}		

		if ( g != null ) {
			context.write(NullWritable.get(), new Text(s + " " + p + " " + o + " " + g));				
		} else if ( ( s != null ) && ( p != null ) && ( o != null ) ) {
			context.write(NullWritable.get(), new Text(s + " " + p + " " + o));
		}
	}

}
