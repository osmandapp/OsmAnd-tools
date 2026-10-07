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
