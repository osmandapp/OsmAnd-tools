package net.osmand.obf.preparation;

import net.osmand.binary.CommonWords;
import net.osmand.data.Street;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class AlternativeNameRulesTest {
	@Test
	public void stradaStataleAddsBothIndexedFormsOnlyForStreet() {
		Capture names = new Capture();
		AlternativeNameIndexGenerator<Street> generator = new AlternativeNameIndexGenerator<>(names);
		generator.setLanguageGroup("it", "Italy_lombardia_europe.obf");
		generator.addAlternativeNames("Strada Statale 42 del Tonale", null, new Street(null), 4);
		assertTrue(names.words.contains("ss"));
		assertTrue(names.words.contains("ss42"));
		assertFalse(names.words.contains("strada"));
	}

	@Test
	public void osmNameTagsSelectOnlyTheirLanguage() {
		Capture names = new Capture();
		AlternativeNameIndexGenerator<Street> generator = new AlternativeNameIndexGenerator<>(names);
		generator.setLanguageGroup("it", "Italy_lombardia_europe.obf");
		assertEquals("it_IT", generator.getMapLocale());
		Street street = new Street(null);
		generator.addAlternativeNames("Strada Statale 42", "old_name:hr", street, 4);
		generator.addAlternativeNames("Strada Statale 42", "int_name", street, 4);
		assertFalse(names.words.contains("ss"));
		generator.addAlternativeNames("Strada Statale 42", "name:it", street, 4);
		assertTrue(names.words.contains("ss"));
	}

	@Test
	public void tagWithoutLanguageSuffixIsInTheLanguageOfTheMap() {
		Capture names = new Capture();
		AlternativeNameIndexGenerator<Street> generator = new AlternativeNameIndexGenerator<>(names);
		generator.setLanguageGroup("it", "Italy_lombardia_europe.obf");
		generator.addAlternativeNames("Strada Statale 42 del Tonale", "official_name", new Street(null), 4);
		assertTrue(names.words.contains("ss42"));
	}

	@Test
	public void germanNameInItalianMapFollowsGermanRules() {
		Capture names = new Capture();
		AlternativeNameIndexGenerator<Street> generator = new AlternativeNameIndexGenerator<>(names);
		generator.setLanguageGroup("it", "Italy_trentino-alto-adige_europe.obf");
		Street street = new Street(null);
		generator.addAlternativeNames("Bahnhofstraße", null, street, 4);
		assertFalse(names.words.contains("bahnhofstr"));
		generator.addAlternativeNames("Bahnhofstraße", "name:de", street, 4);
		assertTrue(names.words.contains("bahnhofstr"));
	}

	@Test
	public void wordOfAnAlternativeNameIsAKeyByItsOwnClass() {
		// "strada" and "statale" are service words of the Italian group: the name is found by "tonale" only
		Capture names = new Capture(CommonWords.getAddrInstance());
		names.setMapName("Italy_lombardia_europe");
		Street street = new Street(null);
		names.addToNameIndex("Strada Statale 42 del Tonale", street, 4, false);
		names.addAlternativeNamesToNameIndex("Strada Statale 42 del Tonale", null, street, 4);
		AlternativeNameIndexGenerator.Stats stats = names.getAlternativeNameStats();
		assertTrue(stats.decisions(KeyDecision.DROPPED_CLASS1) >= 2);
		// "SS42" and "SS" are keys although the words they replace are not
		assertTrue(names.words.contains("ss42"));
		assertTrue(names.words.contains("ss"));
		assertEquals(2, stats.decisions(KeyDecision.ALT));
		// "del" of the alternative names keeps the decision of the name
		assertTrue(stats.decisions(KeyDecision.ALT_SHADOWED) >= 1);
		assertFalse(names.words.contains("del"));
	}

	private static class Capture extends NameIndexCreator<Street> {
		final Set<String> words = new HashSet<>();

		Capture() {
			super(null);
		}

		Capture(CommonWords commonWords) {
			super(commonWords);
		}

		@Override
		AlternativeNameIndexGenerator.KeyOutcome addAlternativeToken(String prefix, Street obj, String word,
				List<String> alternativeWords) {
			return words.add(word) ? AlternativeNameIndexGenerator.KeyOutcome.BLOCK
					: AlternativeNameIndexGenerator.KeyOutcome.DUP;
		}
	}
}
