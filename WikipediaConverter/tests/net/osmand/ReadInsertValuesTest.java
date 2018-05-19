package net.osmand;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.osm.util.WikiDatabasePreparation;
import net.osmand.osm.util.WikiDatabasePreparation.InsertValueProcessor;

public class ReadInsertValuesTest {

	@Test
	public void testDifferentOrientationMultipolygon() throws IOException, XmlPullParserException {
		final String[][] expected = new String[][] {
			new String[] {"11111", "aa", "bbbb"},
			new String[] {"11111", "aa", "bbbb"},
			new String[] {"11112", "a'a", "bb\\'b'b"},
			new String[] {"11113", "a\\", "b)b", "c(c')"},
			new String[] {"11111", "aa", "bbb (bbb bb)"},
			new String[] {"11112", "aa", "bbbb (bbbb(bbbb))"},
			new String[] {"11113", "a'a'a", "b", "((c,c)),"},
		};
		WikiDatabasePreparation.readInsertValuesFile("tests/test_wiki.sql", new InsertValueProcessor() {
			int ind = 0;
			@Override
			public void setLang(String replace) {
			}

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
		WikiDatabasePreparation.readInsertValuesFile("tests/langlinks.sql", new InsertValueProcessor() {
			@Override
			public void setLang(String replace) {
			}

			@Override
			public void process(List<String> vs) {
				Assert.assertEquals(3, vs.size());
			}
		});
	}
}
