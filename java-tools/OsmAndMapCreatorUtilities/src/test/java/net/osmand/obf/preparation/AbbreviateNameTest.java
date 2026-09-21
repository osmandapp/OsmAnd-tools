package net.osmand.obf.preparation;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class AbbreviateNameTest {

	@Test
	public void abbreviatedWord() {
		check("Trinity Place", "Trinity pl");
		check("Grand Parkway", "Grand pkwy");
		check("Mount Vernon", "mt Vernon");
	}

	@Test
	public void everyAbbreviatedWordOfTheName() {
		check("Mount Vernon Parkway", "mt Vernon pkwy");
	}

	/** the abbreviation goes into the index, which matches without case: it is written as the table spells it */
	@Test
	public void caseOfTheNameDoesNotMatter() {
		check("TRINITY PLACE", "TRINITY pl");
	}

	@Test
	public void noAbbreviatedWord() {
		check("Trinity Avenue", null);
		check("Broad Street", null);
		check("Placeholder Road", null);
		check("Mountain View", null);
	}

	private static void check(String name, String expected) {
		assertEquals(name, expected, NameIndexCreator.abbreviateName(name));
	}
}
