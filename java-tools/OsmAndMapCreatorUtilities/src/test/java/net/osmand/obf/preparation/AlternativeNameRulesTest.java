package net.osmand.obf.preparation;

import net.osmand.binary.CommonWords;
import net.osmand.binary.SearchVariantRules;
import net.osmand.data.Street;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class AlternativeNameRulesTest {
	private static final String US_MAP = "Us_texas_northamerica";
	private static final String TEST_FILE = "rules_test.xml";

	private String installedLocale;

	@After
	public void restoreRules() throws Exception {
		if (installedLocale != null) {
			rulesCache().remove(installedLocale);
		}
	}

	@Test
	public void stradaStataleAddsBothIndexedFormsOnlyForStreet() {
		Capture names = new Capture("Italy_lombardia_europe.obf");
		names.addToNameIndex("Strada Statale 42 del Tonale", null, new Street(null), 4, false);
		assertTrue(names.words.contains("ss"));
		assertTrue(names.words.contains("ss42"));
		assertFalse(names.words.contains("strada"));
	}

	@Test
	public void osmNameTagsSelectOnlyTheirLanguage() {
		Capture names = new Capture("Italy_lombardia_europe.obf");
		assertEquals("it_IT", names.alternativeNames.getMapLocale());
		Street street = new Street(null);
		names.addToNameIndex("Strada Statale 42", "old_name:hr", street, 4, false);
		names.addToNameIndex("Strada Statale 42", "int_name", street, 4, false);
		assertFalse(names.words.contains("ss"));
		names.addToNameIndex("Strada Statale 42", "name:it", street, 4, false);
		assertTrue(names.words.contains("ss"));
	}

	@Test
	public void tagWithoutLanguageSuffixIsInTheLanguageOfTheMap() {
		Capture names = new Capture("Italy_lombardia_europe.obf");
		names.addToNameIndex("Strada Statale 42 del Tonale", "official_name", new Street(null), 4, false);
		assertTrue(names.words.contains("ss42"));
	}

	@Test
	public void germanNameInItalianMapFollowsGermanRules() {
		Capture names = new Capture("Italy_trentino-alto-adige_europe.obf");
		Street street = new Street(null);
		names.addToNameIndex("Bahnhofstraße", null, street, 4, false);
		assertFalse(names.words.contains("bahnhofstr"));
		names.addToNameIndex("Bahnhofstraße", "name:de", street, 4, false);
		assertTrue(names.words.contains("bahnhofstr"));
	}

	@Test
	public void wordOfAnAlternativeNameIsAKeyByItsOwnClass() {
		// "strada" and "statale" are service words of the Italian group: the name is found by "tonale" only
		Capture names = new Capture("Italy_lombardia_europe", CommonWords.getAddrInstance());
		Street street = new Street(null);
		names.addToNameIndex("Strada Statale 42 del Tonale", null, street, 4, false);
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

	@Test
	public void attachedWordIsAWordOfTheCommonWordsTable() throws Exception {
		install("en_US", "<index><rule object=\"street\" from=\"\\bInstitute\\b\" to=\"church\"/></index>");
		NameIndexCreator<Street> names = new NameIndexCreator<>(CommonWords.getAddrInstance());
		names.setMapName(US_MAP);
		names.addToNameIndex("Qzx Institute", null, new Street(null), 4, false);
		assertEquals(1, names.getAlternativeNameStats().decisions(KeyDecision.ALT_ATTACHED));
		assertFalse(names.namesIndex.containsKey("chur"));
		NameIndexCreator.PrepareWordsIndex common = names.buildCommonWords(names.namesIndex);
		// the atom of "qzx" refers to "church" in the table instead of counting it as an unknown other word
		assertTrue(common.words().containsKey("church"));
		NameIndexCreator.NamedObjectsByPrefix<Street> qzx = names.namesIndex.get("qzx");
		qzx.build(common);
		for (NameIndexCreator.NamedObject<Street> o : qzx.namedObjects) {
			assertFalse(o.isOtherWordsNonZeros());
		}
		// not counted as a non-indexed occurrence: only the words the statistics dropped are
		assertEquals(0, common.words().get("church").nonindexed());
	}

	@Test
	public void alternativeNameFollowsTheNumberPolicyOfTheName() throws Exception {
		install("en_US", "<index><rule object=\"street\" from=\"\\bHighway\\b\" to=\"42\"/></index>");
		NameIndexCreator<Street> names = new NameIndexCreator<>(CommonWords.getAddrInstance());
		names.setMapName(US_MAP);
		names.addToNameIndex("Highway Zeroth", null, new Street(null), 4, false);
		assertFalse(names.namesIndex.containsKey("42"));
		assertEquals(1, names.getAlternativeNameStats().decisions(KeyDecision.NUMBER));
		// a name with numbers (a postcode) keeps it as a key
		NameIndexCreator<Street> numbers = new NameIndexCreator<>(CommonWords.getAddrInstance());
		numbers.setMapName(US_MAP);
		numbers.addToNameIndex("Highway Zeroth", null, new Street(null), 4, true);
		assertTrue(numbers.namesIndex.containsKey("42"));
	}

	@Test
	public void statisticsTellRulesApartByFileObjectAndFrom() throws Exception {
		install("en_US", "<index>"
				+ "<rule object=\"street\" from=\"\\bQzx\\s+Road\\b\" to=\"Qzxway\"/>"
				+ "<rule object=\"street\" from=\"\\bQzx\\s+Lane\\b\" to=\"Qzxway\"/>"
				+ "<unglue glue=\".\"/><unglue glue=\"'\"/></index>");
		Capture names = new Capture(US_MAP);
		Street street = new Street(null);
		names.addToNameIndex("Qzx Road", null, street, 4, false);
		names.addToNameIndex("Qzx Lane", null, street, 4, false);
		names.addToNameIndex("Ab.Cdef Ghij'Kl", null, street, 4, false);
		Map<String, AlternativeNameIndexGenerator.KeyStats> byRule = names.getAlternativeNameStats().byRule;
		// one target, two rules: two lines
		assertEquals(1, byRule.get(TEST_FILE + " street \\bQzx\\s+Road\\b").alternatives);
		assertEquals(1, byRule.get(TEST_FILE + " street \\bQzx\\s+Lane\\b").alternatives);
		// every <unglue> gives its own alternative name
		assertEquals(1, byRule.get(TEST_FILE + " unglue .").alternatives);
		assertEquals(1, byRule.get(TEST_FILE + " unglue '").alternatives);
		assertFalse(byRule.toString(), byRule.containsKey(TEST_FILE + " unglue .'"));
	}

	@Test
	public void alternativeOfWordsOfTheNameIsStoredUnderItsKeys() {
		// "E.T.A. Hoffmann" -> "Hoffmann": no new word, the alternative name is one more name of the atom of "hoffmann"
		NameIndexCreator<Street> names = new NameIndexCreator<>(CommonWords.getAddrInstance());
		names.setMapName(US_MAP);
		names.addToNameIndex("E.T.A. Hoffmann", null, new Street(null), 4, false);
		AlternativeNameIndexGenerator.KeyStats rule = names.getAlternativeNameStats().byRule.get("rules.xml unglue .");
		assertEquals(1, rule.alternatives);
		assertEquals(1, rule.keys());
		NameIndexCreator.PrepareWordsIndex common = names.buildCommonWords(names.namesIndex);
		NameIndexCreator.NamedObjectsByPrefix<Street> hoff = names.namesIndex.get("hoff");
		hoff.build(common);
		NameIndexCreator.NamedObject<Street> atom = hoff.namedObjects.get(0);
		int alone = -1;
		for (int i = 0; i < atom.singleNames.size(); i++) {
			if (atom.singleNames.get(i).listNames().equals(List.of("hoffmann"))) {
				alone = i;
			}
		}
		assertTrue(atom.singleNames.toString(), alone >= 0);
		// the words the rule removed do not count against the object
		assertEquals(0, atom.otherWordsCount.get(alone));
	}

	@Test
	public void oneAlternativeOfTwoRulesIsCountedForEach() throws Exception {
		install("en_US", "<index><unglue glue=\".\" minPart=\"3\"/><unglue glue=\"'\" minPart=\"3\"/></index>");
		NameIndexCreator<Street> names = new NameIndexCreator<>(CommonWords.getAddrInstance());
		names.setMapName(US_MAP);
		names.addToNameIndex("A.'B Hoffmann", null, new Street(null), 4, false);
		AlternativeNameIndexGenerator.Stats stats = names.getAlternativeNameStats();
		assertEquals(1, stats.alternatives);
		for (String id : List.of(TEST_FILE + " unglue .", TEST_FILE + " unglue '")) {
			assertEquals(id, 1, stats.byRule.get(id).alternatives);
			assertEquals(id, 1, stats.byRule.get(id).keys());
		}
		assertEquals(stats.byRule.toString(), 2, stats.byRule.size());
	}

	@Test
	public void decisionsFollowThePriorityOfTheSpec() throws Exception {
		// a new word of <class0>: ALWAYS, not ALT
		install("en_US", "<index><class0>qzxway</class0>"
				+ "<rule object=\"street\" from=\"\\bQzx\\s+Road\\b\" to=\"Qzxway\"/>"
				+ "<rule object=\"locality\" from=\"\\bQzxtown\\b\" to=\"Qzxton\" keys=\"always\"/></index>");
		NameIndexCreator<net.osmand.data.MapObject> names = new NameIndexCreator<>(CommonWords.getAddrInstance());
		names.setMapName(US_MAP);
		names.addToNameIndex("Qzx Road", null, new Street(null), 4, false);
		AlternativeNameIndexGenerator.Stats stats = names.getAlternativeNameStats();
		assertEquals(1, stats.decisions(KeyDecision.ALWAYS));
		assertEquals(0, stats.decisions(KeyDecision.ALT));
		// a notable object with keys="always": NOTABLE comes first
		names.addToNameIndex("Qzxtown", null, new net.osmand.data.City(net.osmand.data.City.CityType.CITY), 4, false);
		assertEquals(1, stats.decisions(KeyDecision.ALWAYS));
		assertEquals(2, stats.decisions(KeyDecision.NOTABLE));
		// a notable object of a map without a group: NOTABLE, not KEPT
		NameIndexCreator<net.osmand.data.MapObject> noGroup = new NameIndexCreator<>(CommonWords.getAddrInstance());
		noGroup.setMapName("Zzqx_europe");
		noGroup.addToNameIndex("Qzx Town", null, new net.osmand.data.City(net.osmand.data.City.CityType.CITY), 4, false);
		assertEquals(2, noGroup.getAlternativeNameStats().decisions(KeyDecision.NOTABLE));
		assertEquals(0, noGroup.getAlternativeNameStats().decisions(KeyDecision.KEPT));
	}

	@Test
	public void mirrorPairAddsAlternativeNamesBothWays() throws Exception {
		install("en_US", "<rule object=\"street\" from=\"blvd\" to=\"Boulevard\"/>");
		NameIndexCreator<Street> names = new NameIndexCreator<>(CommonWords.getAddrInstance());
		names.setMapName(US_MAP);
		names.addToNameIndex("Sunset Boulevard", null, new Street(null), 4, false);
		names.addToNameIndex("Hollywood Blvd.", null, new Street(null), 4, false);
		Map<String, AlternativeNameIndexGenerator.KeyStats> byRule = names.getAlternativeNameStats().byRule;
		assertEquals(1, byRule.get(TEST_FILE + " street Boulevard→blvd").alternatives);
		assertEquals(1, byRule.get(TEST_FILE + " street blvd→Boulevard").alternatives);
		// "blvd" is always a key: "Sunset Boulevard" is found by it now
		assertTrue(names.namesIndex.containsKey("blvd"));
		NameIndexCreator.NamedObjectsByPrefix<Street> blvd = names.namesIndex.get("blvd");
		boolean sunset = false;
		for (NameIndexCreator.NamedObject<Street> o : blvd.namedObjects) {
			for (NameIndexCreator.NameObjectSingleNameIndex n : o.singleNames) {
				sunset |= n.listNames().equals(List.of("sunset", "blvd"));
			}
		}
		assertTrue(sunset);
		// a poi is no street: the pair does not apply
		NameIndexCreator<NameIndexCreator.PoiNameObject> pois = new NameIndexCreator<>(CommonWords.getAddrInstance());
		pois.setMapName(US_MAP);
		pois.addToNameIndex("Boulevard Cafe", null, new NameIndexCreator.PoiNameObject(null, 0, -1, "cafe", "cafe",
				null, null, false), 4, false);
		assertEquals(0, pois.getAlternativeNameStats().alternatives);
	}

	@Test
	public void mirrorWordIsNoKeyWhereTheNameDroppedItsPair() throws Exception {
		install("en_US", "<rule from=\"ave\" to=\"Avenue\"/>");
		// "avenue" (class 1) is dropped next to the other words; "ave" (class 2) has no word 10 times rarer in
		// "Route 4 East At Forest Ave" and would be a key of every such Avenue
		NameIndexCreator<Street> names = new NameIndexCreator<>(CommonWords.getAddrInstance());
		names.setMapName(US_MAP);
		names.addToNameIndex("Route 4 East At Forest Avenue", null, new Street(null), 4, false);
		AlternativeNameIndexGenerator.Stats stats = names.getAlternativeNameStats();
		assertFalse(names.namesIndex.containsKey("aven"));
		assertFalse(names.namesIndex.containsKey("ave"));
		assertEquals(1, stats.decisions(KeyDecision.ALT_ATTACHED));
		assertEquals(0, stats.decisions(KeyDecision.ALT));
		// the name keeps "avenue" as a key: its pair is decided by its own class
		NameIndexCreator<Street> alone = new NameIndexCreator<>(CommonWords.getAddrInstance());
		alone.setMapName(US_MAP);
		alone.addToNameIndex("Avenue", null, new Street(null), 4, false);
		assertTrue(alone.namesIndex.containsKey("aven"));
		assertTrue(alone.namesIndex.containsKey("ave"));
	}

	@Test
	public void indexRuleOfAWordIsNoKeyWhereTheNameDroppedTheWord() throws Exception {
		// the pair moved to <index> as two rules: the word "av" stands for "avenue" as the mirror word does
		install("en_US", "<index><rule object=\"street\" mode=\"All\" from=\"(?iu)\\bAvenue\\b\" to=\"av\"/></index>");
		NameIndexCreator<Street> names = new NameIndexCreator<>(CommonWords.getAddrInstance());
		names.setMapName(US_MAP);
		names.addToNameIndex("Route 4 East At Forest Avenue", null, new Street(null), 4, false);
		assertFalse(names.namesIndex.containsKey("av"));
		assertEquals(1, names.getAlternativeNameStats().decisions(KeyDecision.ALT_ATTACHED));
		assertEquals(0, names.getAlternativeNameStats().decisions(KeyDecision.ALT));
	}

	@Test
	public void attachedReplacedWordIsANameUnderTheKeysOfTheName() throws Exception {
		install("en_US", "<index><rule object=\"street\" mode=\"All\" from=\"(?iu)\\bPlace\\b\" to=\"pl\"/></index>");
		NameIndexCreator<Street> names = new NameIndexCreator<>(CommonWords.getAddrInstance());
		names.setMapName(US_MAP);
		names.addToNameIndex("Trinity Place", null, new Street(null), 4, false);
		// "place" (class 1) is dropped: "pl" is no key but a word of the common words table
		assertEquals(Set.of("trin"), names.namesIndex.keySet());
		NameIndexCreator.PrepareWordsIndex common = names.buildCommonWords(names.namesIndex);
		assertTrue(common.words().containsKey("pl"));
		NameIndexCreator.NamedObjectsByPrefix<Street> trin = names.namesIndex.get("trin");
		trin.build(common);
		boolean alternative = false;
		for (NameIndexCreator.NamedObject<Street> o : trin.namedObjects) {
			for (NameIndexCreator.NameObjectSingleNameIndex n : o.singleNames) {
				alternative |= n.listNames().equals(List.of("trinity", "pl"));
			}
		}
		// "Trinity Pl" is one more name of the atom of "trinity"
		assertTrue(alternative);
	}

	@Test
	public void replacedWordsAreOneForOne() {
		assertEquals(Map.of("ave", "avenue"),
				AlternativeNameIndexGenerator.replacedWords("Route 4 East At Forest Avenue", "Route 4 East At Forest Ave"));
		assertEquals(Map.of("blvd", "boulevard", "e", "east"),
				AlternativeNameIndexGenerator.replacedWords("East Sunset Boulevard", "E Sunset Blvd"));
		// a phrase becomes one word: no word it stands for
		assertEquals(Map.of(), AlternativeNameIndexGenerator.replacedWords("Strada Statale 42 del Tonale",
				"SS42 del Tonale"));
	}

	private void install(String locale, String body) throws Exception {
		Method parse = SearchVariantRules.class.getDeclaredMethod("parseLayer", InputStream.class, String.class);
		parse.setAccessible(true);
		Object layer = parse.invoke(null, new ByteArrayInputStream(("<rules version=\"5\">" + body + "</rules>")
				.getBytes(StandardCharsets.UTF_8)), TEST_FILE);
		Method of = SearchVariantRules.class.getDeclaredMethod("of", String.class, List.class);
		of.setAccessible(true);
		rulesCache().put(locale, (SearchVariantRules) of.invoke(null, locale, List.of(layer)));
		installedLocale = locale;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, SearchVariantRules> rulesCache() throws Exception {
		Field cache = SearchVariantRules.class.getDeclaredField("CACHE");
		cache.setAccessible(true);
		return (Map<String, SearchVariantRules>) cache.get(null);
	}

	private static class Capture extends NameIndexCreator<Street> {
		final Set<String> words = new HashSet<>();

		Capture(String mapName) {
			this(mapName, null);
		}

		Capture(String mapName, CommonWords commonWords) {
			super(commonWords);
			setMapName(mapName);
		}

		@Override
		AlternativeNameIndexGenerator.KeyOutcome addAlternativeToken(String prefix, Street obj, String word,
				List<String> alternativeWords) {
			return words.add(word) ? AlternativeNameIndexGenerator.KeyOutcome.BLOCK
					: AlternativeNameIndexGenerator.KeyOutcome.DUP;
		}
	}
}
