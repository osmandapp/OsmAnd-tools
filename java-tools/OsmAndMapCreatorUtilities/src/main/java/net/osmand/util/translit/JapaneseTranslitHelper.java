package net.osmand.util.translit;

import java.util.List;
import com.atilika.kuromoji.ipadic.Token;
import com.atilika.kuromoji.ipadic.Tokenizer;
import net.osmand.util.translit.japanese.NumberCreator;
import net.osmand.util.translit.japanese.JapaneseMapper;

public class JapaneseTranslitHelper {

	private static Tokenizer tokenizer;
	
	private static final String SPACE = " ";
	private static final String JAPANESE_NUMBER = "数";
	private static final String JAPANESE_SYMBOL = "記号";
	private static final String UNDEFINED_VALUE = "アルファベット";
	
	public static String getEnglishTransliteration(String text) {
		return japanese2Romaji(text);
	}
	JapaneseTranslitHelper(){}

	private static String japanese2Romaji(String text) {
		if (tokenizer == null) {
			tokenizer = new Tokenizer();
		}
		boolean capitalizeWords = true;
		List<Token> tokens = tokenizer.tokenize(text);
		StringBuilder builder = new StringBuilder();
		StringBuilder number = new StringBuilder();
		for (Token token : tokens) {
			if (token.getAllFeaturesArray()[0].equals(JAPANESE_SYMBOL)) {
				builder.append(token.getSurface());
				continue;
			}
			switch (token.getAllFeaturesArray()[1]) {
			case JAPANESE_NUMBER:
				number.append(token.getSurface());
				if (tokens.indexOf(token) == tokens.size() - 1) {
					builder.append(NumberCreator.convertNumber(number.toString()));
				}
				continue;
			case UNDEFINED_VALUE:
				builder.append(token.getSurface()).append(SPACE);
				continue;
			default:
				if (!number.toString().equals("")) {
					builder.append(NumberCreator.convertNumber(number.toString())).append(SPACE);
					number = new StringBuilder();
				}
				String lastFeature = token.getAllFeaturesArray()[8];
				if (lastFeature.equals("*")) {
					builder.append(token.getSurface());
				} else {
					String romaji = convertKanaToRomaji(token.getAllFeaturesArray()[8]);
					if (capitalizeWords) {
						builder.append(romaji.substring(0, 1).toUpperCase());
						builder.append(romaji.substring(1));
					} else {
						if (token.getSurface().equals(token.getPronunciation())) {
							romaji = romaji.toUpperCase();
						}
						builder.append(romaji);
					}
				}
			}
			builder.append(SPACE);
		}
		return builder.toString().trim();
	}
	
	private static String createLongSounds(String romaji) {
		StringBuilder res = new StringBuilder();
		char hyphen = '-';
		for (int i = 0; i < romaji.length(); i++) {
			char curr = romaji.charAt(i);
			if (i + 1 < romaji.length()) {
				char next = romaji.charAt(i + 1);
				if (next == hyphen && JapaneseMapper.isLongSoundLetter(curr)) {
					res.append(JapaneseMapper.getLetterWithDiacritic(curr));
					i++;
					continue;
				}
			}
			res.append(curr);
		}
		return res.toString();
	}

	private static String convertKanaToRomaji(String s) {
		StringBuilder t = new StringBuilder();
		for (int i = 0; i < s.length(); i++) {
			if (i <= s.length() - 2) {
				if (JapaneseMapper.isKatakana(s.substring(i, i + 2))) {
					t.append(JapaneseMapper.getRomaji(s.substring(i, i + 2)));
					i++;
				} else if (JapaneseMapper.isKatakana(s.substring(i, i + 1))) {
					t.append(JapaneseMapper.getRomaji(s.substring(i, i + 1)));
				} else if (s.charAt(i) == 'ッ') {
					t.append(JapaneseMapper.getRomaji(s.substring(i + 1, i + 2)).charAt(0));
				} else {
					t.append(s.charAt(i));
				}
			} else {
				if (JapaneseMapper.isKatakana(s.substring(i, i + 1))) {
					t.append(JapaneseMapper.getRomaji(s.substring(i, i + 1)));
				} else {
					t.append(s.charAt(i));
				}
			}
		}
		String res = t.toString();
		return res.contains("-") ? createLongSounds(res) : res;
	}
}
