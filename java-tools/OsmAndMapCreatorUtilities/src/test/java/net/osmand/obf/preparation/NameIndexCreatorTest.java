package net.osmand.obf.preparation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.junit.Test;

import net.osmand.binary.CommonWords;
import net.osmand.binary.NameIndexReader;
import net.osmand.data.City;
import net.osmand.data.City.CityType;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.util.ArabicNormalizer;
import net.osmand.util.SearchAlgorithms;

public class NameIndexCreatorTest {

	@Test
	public void angleBracketsDoNotAffectArabicTokens() {
		String marker = " " + NameIndexReader.CITY_AS_STREET_COMMON;
		for (String name : List.of("روضة", "١٢٣ روضة", "روضة روضة")) {
			for (String suffix : List.of("", " (District)")) {
				String wrapped = "<" + name + ">" + suffix;
				assertTrue(ArabicNormalizer.isSpecialArabic(wrapped));
				assertEquals("<" + ArabicNormalizer.normalize(name) + ">" + suffix,
						ArabicNormalizer.normalize(wrapped));
				for (boolean unique : List.of(false, true)) {
					assertEquals(SearchAlgorithms.splitAndNormalize(name + suffix + marker, unique),
							SearchAlgorithms.splitAndNormalize(wrapped + marker, unique));
				}
			}
		}
	}

	@Test
	public void nonArabicAngleBracketsAreNotSpecialArabic() {
		for (String name : List.of("<", "<>", "<London>", "<London> روضة")) {
			assertFalse(ArabicNormalizer.isSpecialArabic(name));
		}
	}

	@Test
	public void distinctArabicPrefixesWithArabicLocale() {
		checkDistinctArabicPrefixes(Locale.forLanguageTag("ar-SA"));
	}

	@Test
	public void distinctArabicPrefixesWithEnglishLocale() {
		checkDistinctArabicPrefixes(Locale.US);
	}

	private static void checkDistinctArabicPrefixes(Locale locale) {
		Locale original = Locale.getDefault();
		Locale originalDisplay = Locale.getDefault(Locale.Category.DISPLAY);
		Locale originalFormat = Locale.getDefault(Locale.Category.FORMAT);
		try {
			Locale.setDefault(locale);
			NameIndexCreator<MapObject> index = new NameIndexCreator<>(CommonWords.getAddrInstance());
			IndexCreatorSettings settings = new IndexCreatorSettings();
			City city = new City(CityType.CITY);
			long id = 1;
			// The address index wraps locality names in angle brackets when storing them as streets.
			for (String name : List.of("<روضت>", "<روضة>")) {
				Street street = new Street(city);
				street.setId(id++);
				street.setName(name);
				street.setName(MapObject.NAME_PLACE_ATTR, "hamlet");
				NameIndexCreator.putAddrNamedMapObject(index, street, 0, settings);
				assertEquals(name, street.getName());
			}
			NameIndexCreator.PrepareWordsIndex commonWords = index.buildCommonWords(index.namesIndex);
			for (NameIndexCreator.NamedObjectsByPrefix<MapObject> entry : index.namesIndex.values()) {
				entry.build(commonWords);
			}
			assertTrue(new HashSet<>(index.namesIndex.keySet()).containsAll(Set.of("روضت", "روضه")));
		} finally {
			Locale.setDefault(original);
			Locale.setDefault(Locale.Category.DISPLAY, originalDisplay);
			Locale.setDefault(Locale.Category.FORMAT, originalFormat);
		}
	}
}
