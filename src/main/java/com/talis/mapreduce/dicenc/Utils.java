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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

	private static final Pattern pattern = Pattern.compile("(?>(?><([^>]*)>)|(?>(_:[^\\s]*)))[\\s]*<([^>]*)>[\\s]*(?>(?>\"([^\"]*)\")|(?><([^>]*)>)|(?>(_:[^\\s]*)))[\\s]*(?><([^>]*)>[\\s]*)?+\\.");
	
	public static String[] parseTripleOrQuad(String line) {
		Matcher matcher = pattern.matcher(line);

		String[] values = null;

		if (matcher.find()) {
			if ( matcher.group(7) != null ) {
				values = new String[4];
				values[3] = matcher.group(7); 
			} else {
				values = new String[3];
			}
			
			values[0] = matcher.group(1);
			if ( values[0] == null ) {
				values[0] = matcher.group(2);
			}
			
			values[1] = matcher.group(3);
			
			values[2] = matcher.group(4);
			if ( values[2] == null ) {
				values[2] = matcher.group(5);
				if ( values[2] == null ) {
					values[2] = matcher.group(6);
				}
			}

//			System.out.println(matcher.group(1)); // URI
//			System.out.println(matcher.group(2)); // bnode
//			System.out.println(matcher.group(3)); // URI
//			System.out.println(matcher.group(4)); // literal
//			System.out.println(matcher.group(5)); // URI
//			System.out.println(matcher.group(6)); // bnode
//			System.out.println(matcher.group(7)); // graph
		
		}
		
		return values;
	}
	
}
