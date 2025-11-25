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
	private static final Map<String, String> specialCodeMap = new HashMap<>() {{
		put("no", "nb");
		put("sh", "sr-Latn");
		put("simple", "en-simple");
		put("mo", "ro");
		put("bh", "bho");
		put("zh-yue", "yue");
		put("zh-yue-hk", "yue-HK");
		put("zh-min-nan", "nan");
		put("zh-min-nan-tw", "nan-TW");
		put("zh-classical", "lzh");
		put("zh-wuu", "wuu");
		put("zh-hakka", "hak");
		put("zh-min-bei", "mnp");
		put("zh-min-dong", "cdo");
		put("zh-gan", "gan");
		put("zh-hsn", "hsn");
		put("zh-cmn", "cmn");
		put("sr-el", "sr-Latn");
		put("sr-ec", "sr-Cyrl");
		put("gan-hans", "gan-Hans");
		put("gan-hant", "gan-Hant");
		put("kk-cyrl", "kk-Cyrl");
		put("kk-latn", "kk-Latn");
		put("kk-arab", "kk-Arab");
		put("kk-cn", "kk-Arab-CN");
		put("kk-kz", "kk-Cyrl-KZ");
		put("kk-tr", "kk-Latn-TR");
		put("ku-latn", "ku-Latn");
		put("ku-arab", "ku-Arab");
		put("tg-cyrl", "tg-Cyrl");
		put("tg-latn", "tg-Latn");
		put("ike-cans", "iu-Cans");
		put("ike-latn", "iu-Latn");
		put("shi-tfng", "shi-Tfng");
		put("shi-latn", "shi-Latn");
		put("als", "gsw");
		put("bat-smg", "sgs");
		put("be-x-old", "be-tarask");
		put("cbk-zam", "cbk");
		put("fiu-vro", "vro");
		put("map-bms", "jv-x-bms");
		put("nrm", "nrf");
		put("roa-rup", "rup");
		put("roa-tara", "nap-x-tara");
		put("tp", "tok");
		put("eml", "egl");
		put("dk", "da");
		put("jp", "ja");
		put("cz", "cs");
		put("ju", "jv");
	}};

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
		if (tag.equals("in") || tag.startsWith("in-")) {
			return "id" + tag.substring(2);
		}
		if (tag.equals("iw") || tag.startsWith("iw-")) {
			return "he" + tag.substring(2);
		}
		if (tag.equals("ji") || tag.startsWith("ji-")) {
			return "yi" + tag.substring(2);
		}
		return tag;
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