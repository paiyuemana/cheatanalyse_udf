package cheat.cheatdomain;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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
//        assertTrue( true );
    	
    }
    public static void main(String args[]){
    	Pattern p = Pattern.compile("(?<=//|)((\\w+)(\\-\\w+)*\\.)+\\w+");
		Matcher m = p.matcher("http://domain.com.cn//aaa.eteaqe.29479387");
		if (m.find()) {
			System.out.println(m.group());
		}else{
			
		}
    }
}
