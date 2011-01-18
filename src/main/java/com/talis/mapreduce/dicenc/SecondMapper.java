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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SecondMapper extends Mapper<LongWritable, Text, Text, Text> {

	private String filename = null;
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) throws IOException ,InterruptedException {
		filename = ((FileSplit)context.getInputSplit()).getPath().getName();
	}
	
	@Override
	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] t = Utils.parseTripleOrQuad(value.toString());
		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("MD5");
			for (String n : t) {
				digest.update((n + "|").getBytes("UTF-8"));
			}
			String hash = new String(Hex.encodeHex(digest.digest()));

			context.write(new Text(t[0]), new Text(hash + "-s"));
			context.write(new Text(t[1]), new Text(hash + "-p"));
			context.write(new Text(t[2]), new Text(hash + "-o"));				
			if ( t.length == 4 ) {
				context.write(new Text(t[3]), new Text(hash + "-g"));
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}		
	}

//	@Override
//	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        byte b[] = StrUtils.asUTF8bytes(value.toString()) ;
//        ByteArrayInputStream in = new ByteArrayInputStream(b) ;
//        Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(in) ;
//        Tuple<Node> tuple = null;
//		if ( Lang.NTRIPLES.equals(lang) ) {
//	        LangNTriples x = RiotReader.createParserNTriples(tokenizer, null) ;
//	        // x.setProfile(RiotLib.profile(null, false, true, new ErrorHandlerEx())) ;
//	        while ( x.hasNext() ) {
//	        	Triple triple = x.next();
//	        	tuple = Tuple.create(triple.getSubject(), triple.getPredicate(), triple.getObject());
//	        }
//		} else if ( Lang.NQUADS.equals(lang) ) {
//	        LangNQuads x = RiotReader.createParserNQuads(tokenizer, null) ;
//	        // x.setProfile(RiotLib.profile(null, false, true, new ErrorHandlerEx())) ;
//	        while ( x.hasNext() ) {
//	        	Quad quad = x.next();
//	        	tuple = Tuple.create(quad.getSubject(), quad.getPredicate(), quad.getObject(), quad.getGraph());
//	        }
//		} else {
//			// TODO
//		}
//
//		String hash = md5(tuple);
//		context.write(new Text(NodeFmtLib.serialize(tuple.get(0))), new Text(hash + "-s"));
//		context.write(new Text(NodeFmtLib.serialize(tuple.get(1))), new Text(hash + "-p"));
//		context.write(new Text(NodeFmtLib.serialize(tuple.get(2))), new Text(hash + "-o"));
//		if (Lang.NQUADS.equals(lang)) {
//			context.write(new Text(NodeFmtLib.serialize(tuple.get(3))), new Text(hash + "-g"));
//		}
//	}
//	
//	private String md5(Tuple<Node> tuple) {
//		MessageDigest digest = null;
//		try {
//	        digest = MessageDigest.getInstance("MD5");
//			for (Node node : tuple) {
//		        NodeType nt = NodeType.lookup(node) ;
//		        switch(nt) {
//		            case URI:
//		            	digest.update((node.getURI() + "|||" + nt.getName()).getBytes("UTF8"));
//		                break ;
//		            case BNODE:
//		            	// TODO: ADD FILENAME HERE!
//		            	digest.update((node.getBlankNodeLabel() + "|||" + nt.getName()).getBytes("UTF8"));
//		                break ;
//		            case LITERAL:
//		            	String lang = node.getLiteralLanguage();
//		            	String datatype = node.getLiteralLanguage();
//		                if ( datatype == null ) datatype = "" ;
//		                if ( lang == null ) lang = "" ;
//		            	digest.update((node.getLiteralLexicalForm() + "|" + lang +  "|" + datatype + "|" + nt.getName()).getBytes("UTF8"));
//		                break ;
//		            case OTHER:
//		            	// TODO
//		                throw new RuntimeException("Attempt to hash something strange: "+node) ; 
//		        }
//			}
//		} catch (UnsupportedEncodingException e) {
//			e.printStackTrace();
//		} catch (NoSuchAlgorithmException e) {
//			e.printStackTrace();
//		}
//		
//		return new String(Hex.encodeHex(digest.digest()));
//	}

}
