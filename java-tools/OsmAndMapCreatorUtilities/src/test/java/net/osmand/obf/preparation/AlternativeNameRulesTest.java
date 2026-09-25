package net.osmand.obf.preparation;

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

	private static class Capture extends NameIndexCreator<Street> {
		final Set<String> words = new HashSet<>();

		Capture() {
			super(null);
		}

		@Override
		void addAlternativeToken(String prefix, Street obj, String word, List<String> alternativeWords) {
			words.add(word);
		}
	}
}
