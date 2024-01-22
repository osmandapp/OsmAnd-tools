package net.osmand.util.translit;

import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.HanyuPinyinVCharType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.StringJoiner;

import static net.sourceforge.pinyin4j.format.HanyuPinyinCaseType.LOWERCASE;
import static net.sourceforge.pinyin4j.format.HanyuPinyinToneType.*;
import static net.sourceforge.pinyin4j.format.HanyuPinyinVCharType.*;

public class ChineseTranslitHelper {

	private ChineseTranslitHelper() {
	}

	private static final String CHINESE_LETTERS = "[\\u4E00-\\u9FA5]+";
	private static final String DELIMITER = " ";

	public static String getPinyinTransliteration(String name) {
		return nameToPinyin(name, getToneMarkPinyinFormat());
	}

	public static String nameToPinyin(String name, HanyuPinyinOutputFormat pinyinFormat) {
		StringBuilder pinyinWords = new StringBuilder();
		boolean prevChinese = false;
		try {
			for (char symbol : name.toCharArray()) {
				String symbolStr = Character.toString(symbol);
				if (prevChinese) {
					pinyinWords.append(DELIMITER);
					prevChinese = false;
				}
				if (symbolStr.matches(CHINESE_LETTERS)) {
					String[] py = PinyinHelper.toHanyuPinyinStringArray(symbol, pinyinFormat);
					if (py != null && py.length > 0) {
						pinyinWords.append(py[0]);
						prevChinese = true;
					}
				} else {
					pinyinWords.append(symbolStr);
				}
			}
		} catch (BadHanyuPinyinOutputFormatCombination e) {
			e.printStackTrace();
		}
		return pinyinWords.toString();
	}

	private static HanyuPinyinOutputFormat getToneMarkPinyinFormat() {
		return getPinyinFormat(LOWERCASE, WITH_TONE_MARK, WITH_U_UNICODE);
	}

	public static HanyuPinyinOutputFormat getPinyinFormat(HanyuPinyinCaseType caseType, HanyuPinyinToneType toneType,
			HanyuPinyinVCharType charType) {
		HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
		format.setCaseType(caseType);
		format.setToneType(toneType);
		format.setVCharType(charType);
		return format;
	}
}
