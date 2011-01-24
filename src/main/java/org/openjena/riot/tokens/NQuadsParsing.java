package org.openjena.riot.tokens;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.openjena.atlas.io.PeekReader;
import org.openjena.riot.Lang;
import org.openjena.riot.lang.LangNQuads;
import org.openjena.riot.system.RiotLib;

import com.hp.hpl.jena.sparql.core.Quad;

public class NQuadsParsing {

	public static void main(String[] args) throws IOException {
		InputStream in = new FileInputStream("src/test/resources/input/data.nq");
        PeekReader peekReader = PeekReader.makeUTF8(in) ;
        Tokenizer tokenizer = new TokenizerText(peekReader) ;
        LangNQuads parser = new LangNQuads(tokenizer, RiotLib.profile(Lang.NQUADS, null), null) ;
        while ( parser.hasNext() ) {
        	System.out.print(peekReader.getLineNum() + " " + (peekReader.getPosition() - peekReader.getColNum() + 1));
        	Quad quad = parser.next();
        	System.out.println(quad);
        }
        in.close();
        
        in = new FileInputStream("src/test/resources/input/data.nq");
        LineReader lr = new LineReader(in);
        in.skip(486);
        Text line = new Text();
        lr.readLine(line);
        System.out.println("[" + line.toString() + "]");
	}

}
