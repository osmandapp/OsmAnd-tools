package net.osmand.osm.util;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import junit.framework.Assert;
import net.osmand.osm.util.WikipediaByCountryDivider.InsertValueProcessor;

public class WikipediaByCountryDividerTest {

	@Test
	public void testValueGroupProcessing() {
		
		final List<List<String>> expectedOutputs = new ArrayList<List<String>>();
		expectedOutputs.add(Arrays.asList("121", "af" , "abcd"));
		expectedOutputs.add(Arrays.asList("121", "af", "ab\'cd"));
		expectedOutputs.add(Arrays.asList("121", "af" , "abcd (abc)"));
		expectedOutputs.add(Arrays.asList("121", "af" , "abcd (abc)(abcd)"));
		expectedOutputs.add(Arrays.asList("121", "af" , "abcd (abc(abcd))"));
		expectedOutputs.add(Arrays.asList("121", "a\'f" , "abcd"));
		expectedOutputs.add(Arrays.asList("123", "a\\'f\\'" , "abcdef"));
		InsertValueProcessor p = new InsertValueProcessor() {
			int testCase = 0;
			@Override
			public void process(List<String> vs) {
				Assert.assertEquals(expectedOutputs.get(testCase).toString(), vs.toString());
				testCase++;
			}
		};
		WikipediaByCountryDivider.processValueGroup(p, "121,'af','abcd'");
		WikipediaByCountryDivider.processValueGroup(p, "121,'af','ab\'cd'");
		WikipediaByCountryDivider.processValueGroup(p, "121,'af','abcd (abc)'");
		WikipediaByCountryDivider.processValueGroup(p, "121,'af','abcd (abc)(abcd)'");
		WikipediaByCountryDivider.processValueGroup(p, "121,'af','abcd (abc(abcd))'");
		WikipediaByCountryDivider.processValueGroup(p, "121,'a\'f','abcd'");
		WikipediaByCountryDivider.processValueGroup(p, "123,'a\\'f\\'','abcdef'");
	}
	
	@Test
	public void testFileProcessing() throws FileNotFoundException, UnsupportedEncodingException, IOException {
		String testFile = getClass().getResource("test_wiki.sql").getPath();
		final List<List<String>> expectedOutputs = new ArrayList<List<String>>();
		expectedOutputs.add(Arrays.asList("14965","aa", "abcd"));
		expectedOutputs.add(Arrays.asList("14986","a\\'a", "abc\\'d"));
		expectedOutputs.add(Arrays.asList("14965","aa","abcd (defg)"));
		expectedOutputs.add(Arrays.asList("14986","aa","abcd (defg(ascd))"));
		expectedOutputs.add(Arrays.asList("1", "2\\'3\\'4", "3"));
		expectedOutputs.add(Arrays.asList("299236", "af", "Sjabloon:\\\\"));
		InsertValueProcessor p = new InsertValueProcessor() {
			int testCase = 0;
			@Override
			public void process(List<String> vs) {
				if (testCase < expectedOutputs.size()) {
					Assert.assertEquals(expectedOutputs.get(testCase).toString(), vs.toString());
					testCase++;
				}
				
			}
		};
		WikipediaByCountryDivider.readInsertValuesFile(testFile, p);
	}

}
