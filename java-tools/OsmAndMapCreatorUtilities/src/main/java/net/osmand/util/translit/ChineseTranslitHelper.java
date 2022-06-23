package net.osmand.util.translit;

import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.HanyuPinyinVCharType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;
import org.apache.commons.lang3.StringUtils;

import java.util.StringJoiner;

import static net.sourceforge.pinyin4j.format.HanyuPinyinCaseType.LOWERCASE;
import static net.sourceforge.pinyin4j.format.HanyuPinyinToneType.*;
import static net.sourceforge.pinyin4j.format.HanyuPinyinVCharType.*;

public class ChineseTranslitHelper {
    
    private ChineseTranslitHelper() {}
    
    private static final String CHINESE_LETTERS = "[\\u4E00-\\u9FA5]+";
    private static final String DELIMITER = " ";
    
    public static String getPinyinTransliteration(String name) {
        return nameToPinyin(name, getToneMarkPinyinFormat());
    }
    
    public static String nameToPinyin(String name, HanyuPinyinOutputFormat pinyinFormat) {
        StringJoiner pinyinWords = new StringJoiner(DELIMITER);
        try {
            for (char word : name.toCharArray()) {
                String wordStr = Character.toString(word);
                if (wordStr.matches(CHINESE_LETTERS)) {
                    String[] py = PinyinHelper.toHanyuPinyinStringArray(word, pinyinFormat);
                    pinyinWords.add(StringUtils.join(py));
                } else {
                    pinyinWords.add(wordStr);
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
    
    public static HanyuPinyinOutputFormat getPinyinFormat(HanyuPinyinCaseType caseType, HanyuPinyinToneType toneType, HanyuPinyinVCharType charType) {
        HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
        format.setCaseType(caseType);
        format.setToneType(toneType);
        format.setVCharType(charType);
        return format;
    }
}
