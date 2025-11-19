package net.osmand.wiki.commonswiki;

import java.util.*;

public class WikiToBcp47 {

	private static final Map<String, String> SPECIAL_MAP = Map.ofEntries(
			Map.entry("sr-el", "sr-Latn"),
			Map.entry("sr-ec", "sr-Cyrl"),
			Map.entry("zh-yue", "yue"),
			Map.entry("zh-yue-hk", "yue-HK"),
			Map.entry("zh-min-nan", "nan"),
			Map.entry("zh-classical", "lzh"),
			Map.entry("zh-wuu", "wuu"),
			Map.entry("zh-hakka", "hak"),
			Map.entry("zh-cmn", "cmn"),
			Map.entry("zh-gan", "gan"),
			Map.entry("zh-hsn", "hsn"),
			Map.entry("als", "gsw"),
			Map.entry("fiu-vro", "vro"),
			Map.entry("bat-smg", "smg"),
			Map.entry("be-x-old", "be-Latn"),
			Map.entry("simple", "en")
	);

	private static final String BCP47_REGEX =
			"^[a-zA-Z]{2,3}" +                         // language
					"(?:-[A-Z][a-z]{3})?" +            // script
					"(?:-[A-Z]{2}|-[0-9]{3})?" +       // region
					"(?:-[a-zA-Z0-9]{5,8})*$";         // variant

	public static boolean isValidBCP47(String tag) {
		if (tag == null || tag.isEmpty()) return false;
		if (!tag.matches(BCP47_REGEX)) return false;
		return Locale.forLanguageTag(tag).toLanguageTag().equalsIgnoreCase(tag);
	}

	public static String convertWikiToBCP47(String wiki) {
		if (wiki == null || wiki.isEmpty()) return "en";

		wiki = wiki.replace('_', '-').toLowerCase(Locale.ROOT);

		if (SPECIAL_MAP.containsKey(wiki))
			return SPECIAL_MAP.get(wiki);

		String[] parts = wiki.split("-");

		if (parts.length == 1) {
			return parts[0];
		}

		String lang = parts[0];
		String script = null;
		String region = null;

		for (int i = 1; i < parts.length; i++) {
			String p = parts[i];
			if (p.length() == 4) {
				script = p.substring(0, 1).toUpperCase() + p.substring(1).toLowerCase();
			} else if (p.equals("el")) {
				script = "Latn";
			} else if (p.equals("ec")) {
				script = "Cyrl";
			} else if (p.length() == 2) {
				region = p.toUpperCase(Locale.ROOT);
			}
		}

		StringBuilder tag = new StringBuilder(lang);
		if (script != null) tag.append("-").append(script);
		if (region != null) tag.append("-").append(region);

		String finalTag = tag.toString();

		return isValidBCP47(finalTag) ? finalTag : lang;
	}
}

