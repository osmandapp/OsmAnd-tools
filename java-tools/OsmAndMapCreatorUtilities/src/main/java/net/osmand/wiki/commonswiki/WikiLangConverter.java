package net.osmand.wiki.commonswiki;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.ibm.icu.util.ULocale;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WikiLangConverter {

	private static final Gson gson = new Gson();
	private static final Type mapType = new TypeToken<Map<String, String>>() {}.getType();
	public static final String UNDEFINED_TAG = "und";
	public static final String UNDEFINED_MARK = "und:"; // used for logging purpose only
	public static boolean DEBUG = false;

	private static final Map<String, String> langCodeCache = new ConcurrentHashMap<>();
	private static final Map<String, String> specialCodeMap = Map.ofEntries(
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
	 * Converts a Wikipedia/Wikimedia language code to a BCP 47 language tag.
	 * <p>
	 * If the code is recognized, returns the corresponding BCP 47 tag.
	 * If the code is not recognized, returns empty string (or "und:" + code in DEBUG mode).
	 * <p>
	 * Examples of conversions:
	 * <ul>
	 *   <li>{@code "no"} &rarr; {@code "nb"}</li>
	 *   <li>{@code "zh-yue"} &rarr; {@code "yue"}</li>
	 *   <li>{@code "be-x-old"} &rarr; {@code "be-tarask"}</li>
	 * </ul>
	 *
	 * @param wikiCode the Wikipedia/Wikimedia language code to convert
	 * @return the BCP 47 language tag, or the empty string if it cannot be recognized
	 * (or prefixed with "und:" in DEBUG mode)
	 */

	public static String toBcp47FromWiki(String wikiCode) {
		return langCodeCache.computeIfAbsent(wikiCode, WikiLangConverter::computeBcp47);
	}

	private static String computeBcp47(String wikiCode) {
		String key = wikiCode.replace('_', '-').toLowerCase(java.util.Locale.ROOT);
		String specialCode = specialCodeMap.get(key);
		if (specialCode != null) {
			return specialCode;
		}
		try {
			ULocale locale = ULocale.forLanguageTag(key);
			String tag = locale.toLanguageTag();
			if (UNDEFINED_TAG.equals(tag)) {
				return DEBUG ? UNDEFINED_MARK + key : "";
			}
			return fixLegacyCodes(tag);
		} catch (Exception e) {
			return DEBUG ? UNDEFINED_MARK + key : "";
		}
	}

	private static String fixLegacyCodes(String tag) {
		// Mapping of deprecated ISO 639 codes to their replacements
		return switch (tag.length() >= 2 ? tag.substring(0, 2) : tag) {
			case "in" -> "id" + tag.substring(2); // Indonesian
			case "iw" -> "he" + tag.substring(2); // Hebrew
			case "ji" -> "yi" + tag.substring(2); // Yiddish
			default -> tag;
		};
	}

	public static String normalizeLang(String jsonStr) {
		if (jsonStr == null || jsonStr.trim().isEmpty()) {
			return "";
		}
		try {
			Map<String, String> parsed = gson.fromJson(jsonStr, mapType);
			if (parsed == null || parsed.isEmpty()) {
				return "";
			}
			Map<String, String> normalized = new HashMap<>();
			for (Map.Entry<String, String> e : parsed.entrySet()) {
				String wikiLangCode = e.getKey().trim();
				String bcp47 = WikiLangConverter.toBcp47FromWiki(wikiLangCode);
				normalized.put(bcp47, e.getValue());
			}
			return gson.toJson(normalized);
		} catch (Exception ex) {
			return jsonStr;
		}
	}
}