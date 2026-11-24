package net.osmand.obf.preparation;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import net.osmand.search.rules.SearchModRules;
import net.osmand.search.rules.SearchModRules.SearchModRuleOwner;
import net.osmand.util.SearchAlgorithms;

/** {@code <rule>} of {@code <index>} as the OBF writer applies it to a name */
public class AlternativeNameRulesTest {

	private static final SearchModRules RULES = new SearchModRules();

	private static List<String> alternatives(String map, String name, String lang, SearchModRuleOwner owner) {
		AlternativeNameIndexGenerator<Object> generator = new AlternativeNameIndexGenerator<>(null);
		generator.setRules(RULES, RULES.locales().forMap(map));
		return List.copyOf(generator.ruleAlternatives(name, lang, owner).values());
	}

	@Test
	public void rulesOfTheMapLocale() {
		assertEquals(List.of("Hauptstr"), alternatives("Germany_bayern_europe", "Hauptstraße", null, SearchModRuleOwner.STREET));
		assertEquals(List.of("Marktpl"), alternatives("Austria_tyrol_europe", "Marktplatz", null, SearchModRuleOwner.STREET));
		// "Strada Statale 42": the road with and without its number
		assertEquals(List.of("SS 42", "SS42"),
				alternatives("Italy_lombardia_europe", "Strada Statale 42", null, SearchModRuleOwner.STREET));
		assertEquals(List.of(), alternatives("Us_new-york_northamerica", "Hauptstraße", null, SearchModRuleOwner.STREET));
	}

	@Test
	public void ownerOfTheName() {
		assertEquals(List.of(), alternatives("Germany_bayern_europe", "Hauptstraße", null, SearchModRuleOwner.POI));
		assertEquals(List.of(), alternatives("Italy_lombardia_europe", "Strada Statale 42", null, SearchModRuleOwner.LOCALITY));
	}

	@Test
	public void languageOfTheName() {
		// name:de of a map of another language follows the German rules
		assertEquals(List.of("Bahnhofstr"), alternatives("Italy_trentino-alto-adige_europe", "Bahnhofstraße", "de",
				SearchModRuleOwner.STREET));
		assertEquals(List.of(), alternatives("Germany_bayern_europe", "Strada Statale 42", "it", SearchModRuleOwner.LOCALITY));
	}

	@Test
	public void replacedWordsOneForOne() {
		assertEquals(Map.of("ave", "avenue"), replaced("Forest Avenue", "Forest Ave"));
		// a phrase has no word for word correspondence
		assertEquals(Map.of(), replaced("Strada Statale 42", "SS42"));
		// a number is a value of its own
		assertEquals(Map.of(), replaced("Highway One", "Highway 1"));
		// one new word for two words of the name
		assertEquals(Map.of(), replaced("Saint Sankt", "St St"));
	}

	private static Map<String, String> replaced(String name, String alternative) {
		return AlternativeNameIndexGenerator.replacedWords(SearchAlgorithms.splitAndNormalize(name, false),
				SearchAlgorithms.splitAndNormalize(alternative, false));
	}
}
