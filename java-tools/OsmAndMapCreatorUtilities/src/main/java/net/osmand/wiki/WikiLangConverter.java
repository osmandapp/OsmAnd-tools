package net.osmand.wiki;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.ibm.icu.util.ULocale;
import net.osmand.util.Algorithms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Converter for Wikipedia language codes to BCP 47 format.
 * <p>
 * This class provides methods to convert Wikipedia language codes (which may include
 * non-standard or legacy codes) to their corresponding BCP 47 language tags.
 * <p>
 * Special mappings are handled via the {@code SPECIAL_MAP}, which contains Wikipedia codes
 * that do not directly map to BCP 47 and require custom conversion.
 * <p>
 * The {@code CACHE} is used to store previously converted codes for performance optimization.
 * <p>
 * Thread safety is guaranteed for the caching mechanism by using a {@link ConcurrentHashMap}.
 */

public class WikiLangConverter {

	public static final String UNDEFINED_TAG = "und";
	public static final String UNDEFINED_MARK = "und:"; // used for logging purpose only
	private static final Logger log = LoggerFactory.getLogger(WikiLangConverter.class);
	public static final int REPLACED_CODE_LENGTH = "in".length();
	public static boolean DEBUG = false;

	private static final Gson gson = new Gson();
	private static final Type mapType = new TypeToken<Map<String, String>>() {
	}.getType();
	private static final Map<String, String> bcp47CodeCache = new ConcurrentHashMap<>();
	private static final Map<String, String> wikiToBcp47Map = Map.ofEntries(
			Map.entry("no", "nb"),
			Map.entry("sh", "sr-Latn"),
			Map.entry("simple", "en-simple"),
			Map.entry("mo", "ro"),
			Map.entry("bh", "bho"),
			Map.entry("zh-yue", "yue"),
			Map.entry("zh-yue-hk", "yue-HK"),
			Map.entry("zh-min-nan", "nan"),
			Map.entry("zh-min-nan-tw", "nan-TW"),
			Map.entry("zh-classical", "lzh"),
			Map.entry("zh-wuu", "wuu"),
			Map.entry("zh-hakka", "hak"),
			Map.entry("zh-min-bei", "mnp"),
			Map.entry("zh-min-dong", "cdo"),
			Map.entry("zh-gan", "gan"),
			Map.entry("zh-hsn", "hsn"),
			Map.entry("zh-cmn", "cmn"),
			Map.entry("sr-el", "sr-Latn"),
			Map.entry("sr-ec", "sr-Cyrl"),
			Map.entry("gan-hans", "gan-Hans"),
			Map.entry("gan-hant", "gan-Hant"),
			Map.entry("kk-cyrl", "kk-Cyrl"),
			Map.entry("kk-latn", "kk-Latn"),
			Map.entry("kk-arab", "kk-Arab"),
			Map.entry("kk-cn", "kk-Arab-CN"),
			Map.entry("kk-kz", "kk-Cyrl-KZ"),
			Map.entry("kk-tr", "kk-Latn-TR"),
			Map.entry("ku-latn", "ku-Latn"),
			Map.entry("ku-arab", "ku-Arab"),
			Map.entry("tg-cyrl", "tg-Cyrl"),
			Map.entry("tg-latn", "tg-Latn"),
			Map.entry("ike-cans", "iu-Cans"),
			Map.entry("ike-latn", "iu-Latn"),
			Map.entry("shi-tfng", "shi-Tfng"),
			Map.entry("shi-latn", "shi-Latn"),
			Map.entry("als", "gsw"),
			Map.entry("bat-smg", "sgs"),
			Map.entry("be-x-old", "be-tarask"),
			Map.entry("cbk-zam", "cbk"),
			Map.entry("fiu-vro", "vro"),
			Map.entry("map-bms", "jv-x-bms"),
			Map.entry("nrm", "nrf"),
			Map.entry("roa-rup", "rup"),
			Map.entry("roa-tara", "nap-x-tara"),
			Map.entry("tp", "tok"),
			Map.entry("eml", "egl"),
			Map.entry("dk", "da"),
			Map.entry("jp", "ja"),
			Map.entry("cz", "cs"),
			Map.entry("ju", "jv")
	);

	/**
	 * Normalizes the language codes in a JSON string mapping language codes to descriptions.
	 * Converts wiki language codes to BCP 47 format.
	 *
	 * @param jsonStr a JSON string containing language code to description mappings
	 * @return a JSON string with language codes converted to BCP 47 format;
	 * returns the original string on parse errors;
	 * returns "" for null or empty input, or if the parsed map is empty
	 */

	public static String normalizeLang(String jsonStr) {
		if (Algorithms.isBlank(jsonStr)) {
			return "";
		}
		try {
			Map<String, String> parsed = gson.fromJson(jsonStr, mapType);
			if (Algorithms.isEmpty(parsed)) {
				return "";
			}
			Map<String, String> normalized = new HashMap<>();
			for (Map.Entry<String, String> e : parsed.entrySet()) {
				String wikiLangCode = e.getKey().trim();
				String description = e.getValue();
				String bcp47Code = null;
				if (!Algorithms.isBlank(description)) {
					bcp47Code = WikiLangConverter.toBcp47FromWiki(wikiLangCode);
				}
				if (!Algorithms.isBlank(bcp47Code)) {
					normalized.put(bcp47Code, description);
				}
			}
			return gson.toJson(normalized);
		} catch (Exception ex) {
			log.info(ex.getMessage());
			return jsonStr;
		}
	}

	/**
	 * Converts a Wikipedia/Wikimedia language code to a BCP 47 language tag.
	 * <p>
	 * If the code is recognized, returns the corresponding BCP 47 tag.
	 * If the code is not recognized, returns null (or "und:" + code in DEBUG mode).
	 * <p>
	 * Examples of conversions:
	 * <ul>
	 *   <li>{@code "no"} &rarr; {@code "nb"}</li>
	 *   <li>{@code "zh-yue"} &rarr; {@code "yue"}</li>
	 *   <li>{@code "be-x-old"} &rarr; {@code "be-tarask"}</li>
	 * </ul>
	 *
	 * @param wikiCode the Wikipedia/Wikimedia language code to convert
	 * @return the BCP 47 language tag, or null if it cannot be recognized
	 * (or prefixed with "und:" in DEBUG mode)
	 */

	public static String toBcp47FromWiki(String wikiCode) {
		return bcp47CodeCache.computeIfAbsent(wikiCode, WikiLangConverter::computeBcp47);
	}

	private static String computeBcp47(String wikiCode) {
		String preparedWikiCode = wikiCode.replace('_', '-').toLowerCase(java.util.Locale.ROOT);
		String bcp47code = wikiToBcp47Map.get(preparedWikiCode);
		if (bcp47code != null) {
			return bcp47code;
		}
		try {
			ULocale uLocale = ULocale.forLanguageTag(preparedWikiCode);
			bcp47code = uLocale.toLanguageTag();
			if (UNDEFINED_TAG.equals(bcp47code)) {
				return debugInfo(wikiCode);
			}
			String fixedBcp47code = fixLegacyCodes(bcp47code);
			return fixedBcp47code != null ? fixedBcp47code : bcp47code;
		} catch (Exception ex) {
			log.info(ex.getMessage());
			return debugInfo(wikiCode);
		}
	}

	private static String debugInfo(String wikiCode) {
		return DEBUG ? UNDEFINED_MARK + wikiCode : null;
	}

	private static String fixLegacyCodes(String tag) {
		// Mapping of deprecated ISO 639 codes to their replacements
		return switch (tag.length() >= REPLACED_CODE_LENGTH ? tag.substring(0, REPLACED_CODE_LENGTH) : tag) {
			case "in" -> "id" + tag.substring(REPLACED_CODE_LENGTH); // Indonesian
			case "iw" -> "he" + tag.substring(REPLACED_CODE_LENGTH); // Hebrew
			case "ji" -> "yi" + tag.substring(REPLACED_CODE_LENGTH); // Yiddish
			default -> null;
		};
	}
}