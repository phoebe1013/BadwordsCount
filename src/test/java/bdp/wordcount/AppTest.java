package bdp.wordcount;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */

    private List<String> filterstopwords(String rawwords){
        List<String> words = new ArrayList<>();
        Analyzer analyzer = new StandardAnalyzer();
        TokenStream stream = null;
        try {
            stream = analyzer.tokenStream("content", new StringReader(rawwords));
            CharTermAttribute attr = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken())
                words.add(attr.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if(stream != null){
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return words;
    }

    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
        String s = "Does anyone have the Jupiter Ascending script? I doubt it, but I really enjoyed the movie and would love to see how it was presented in script form, " +
                "especially since there is so clearly connective tissue missing from the final product. https://blog.csdn.net/xsi640/article/details/30035659?utm_source=blogxgwz1 Thanks!";
        List<String> result = filterstopwords(s);
        System.out.println(result);
    }
}
