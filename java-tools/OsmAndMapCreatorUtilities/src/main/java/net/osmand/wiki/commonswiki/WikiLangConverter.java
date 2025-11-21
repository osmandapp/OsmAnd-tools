package net.osmand.wiki.commonswiki;

import com.ibm.icu.util.ULocale;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class WikiLangConverter {

	private static final Map<String, String> SPECIAL_MAP = new HashMap<>() {{
		// Legacy and non-standard wiki codes
		put("sr-el", "sr-Latn");
		put("sr-ec", "sr-Cyrl");
		put("zh-yue", "yue");
		put("zh-yue-hk", "yue-HK");
		put("zh-min-nan", "nan");
		put("zh-min-nan-tw", "nan-TW");
		put("zh-classical", "lzh");
		put("zh-wuu", "wuu");
		put("zh-hakka", "hak");
		put("zh-cmn", "cmn");
		put("zh-gan", "gan");
		put("zh-hsn", "hsn");
		put("als", "gsw");
		put("fiu-vro", "vro");
		put("bat-smg", "smg");
		put("be-x-old", "be-Latn");
		put("simple", "en");

		// ISO 639-3 non-standard codes
		put("eml", "egl");
		put("xmf", "xmf");
		put("diq", "diq");
		put("pfl", "pfl");
		put("rue", "rue");
		put("lfn", "lfn");
		put("bcl", "bcl");
		put("war", "war");
		put("ilo", "ilo");
		put("roa-tara", "roa-tara");
		put("cbk-zam", "cbk-zam");
		put("map-bms", "map-bms");
	}};

	/**
	 * Converts a Wikipedia language code to a valid BCP-47 tag using ICU4J normalization.
	 *
	 * @param wiki Wikipedia language code (e.g., "sr-el", "zh-min-nan-tw")
	 * @return normalized BCP-47 language tag
	 */
	public static String toBcp47FromWiki(String wiki) {
		if (wiki == null || wiki.isEmpty()) {
			return "en";
		}
		String key = wiki.replace('_', '-').toLowerCase(Locale.ROOT);
		if (SPECIAL_MAP.containsKey(key)) {
			return normalizeWithICU(SPECIAL_MAP.get(key));
		}
		String[] parts = key.split("-");
		if (parts.length == 0) {
			return "en";
		}
		String lang = parts[0];
		String script = null;
		String region = null;
		StringBuilder variants = new StringBuilder();
		for (int i = 1; i < parts.length; i++) {
			String p = parts[i];
			if (p.length() == 4) {
				script = p.substring(0, 1).toUpperCase() + p.substring(1).toLowerCase();
			} else if ("el".equals(p)) {
				script = "Latn";
			} else if ("ec".equals(p)) {
				script = "Cyrl";
			} else if (p.length() == 2 && p.matches("[a-z]{2}")) {
				region = p.toUpperCase(Locale.ROOT);
			} else if (p.length() == 3 && p.matches("\\d{3}")) {
				region = p;
			} else {
				if (!variants.isEmpty()) {
					variants.append("-");
				}
				variants.append(p);
			}
		}
		StringBuilder sb = new StringBuilder(lang);
		if (script != null) {
			sb.append("-").append(script);
		}
		if (region != null) {
			sb.append("-").append(region);
		}
		if (!variants.isEmpty()) {
			sb.append("-").append(variants);
		}

		return normalizeWithICU(sb.toString());
	}

	private static String normalizeWithICU(String tag) {
		try {
			ULocale locale = ULocale.forLanguageTag(tag);
			String normalized = locale.toLanguageTag();
			if (isValidBcp47(normalized)) {
				return normalized;
			}
			return locale.getLanguage();
		} catch (Exception e) {
			return fallbackSimple(tag);
		}
	}

	private static boolean isValidBcp47(String tag) {
		if (tag == null || tag.isEmpty()) {
			return false;
		}
		try {
			ULocale locale = ULocale.forLanguageTag(tag);
			return locale.toLanguageTag().equalsIgnoreCase(tag);
		} catch (Exception e) {
			return false;
		}
	}

	private static String fallbackSimple(String tag) {
		int dash = tag.indexOf('-');
		return (dash >= 0) ? tag.substring(0, dash) : tag;
	}
}
