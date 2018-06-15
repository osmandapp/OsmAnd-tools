package net.osmand.util.sql;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import net.osmand.util.sql.SqlInsertValuesReader;
import net.osmand.util.sql.SqlInsertValuesReader.InsertValueProcessor;
import net.osmand.wiki.creator.WikiDatabasePreparation;

import org.junit.Assert;
import org.junit.Test;
import org.xmlpull.v1.XmlPullParserException;

public class ReadInsertValuesTest {

	@Test
	public void testComplexQuotesSql() throws IOException, XmlPullParserException {
		final String[][] expected = new String[][] {
			new String[] {"11111", "aa", "bbbb"},
			new String[] {"11111", "aa", "bbbb"},
			new String[] {"11112", "a'a", "bb\\'b'b"},
			new String[] {"11113", "a\\", "b)b", "c(c')"},
			new String[] {"11111", "aa", "bbb (bbb bb)"},
			new String[] {"11112", "aa", "bbbb (bbbb(bbbb))"},
			new String[] {"11113", "a'a'a", "b", "((c,c)),"},
		};
		SqlInsertValuesReader.readInsertValuesFile(ReadInsertValuesTest.class.getResourceAsStream("/test_wiki.sql"), 
				new InsertValueProcessor() {
			int ind = 0;
			@Override
			public void process(List<String> vs) {
				System.out.println(ind + " " + Arrays.toString(expected[ind]) +" " + vs);
				String[] array = vs.toArray(new String[vs.size()]);
				Assert.assertArrayEquals(array, expected[ind]);
				ind++;
			}
		});
	}
	
	@Test
	public void testEnglish() throws IOException, XmlPullParserException {
		SqlInsertValuesReader.readInsertValuesFile(
				ReadInsertValuesTest.class.getResourceAsStream("/langlinks.sql"), new InsertValueProcessor() {
			@Override
			public void process(List<String> vs) {
				Assert.assertEquals(3, vs.size());
			}
		});
	}
}
