package net.osmand.util.translit.japanese;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class JapaneseMapper {
    
    private static final Map<String, String> KATAKANA = new HashMap<>();
    private static final Map<Character, String> LONG_SOUND_LETTER = new HashMap<>();
    private static final Map<Character, BigDecimal> NUMBER = new HashMap<>();
    private static final Map<Character, BigDecimal> CARDINAL = new HashMap<>();
    private static final Map<Character, String> DELIMITER = new HashMap<>();
    private static final Map<Character, String> DECIMAL = new HashMap<>();
    
    static {
        initKanaMap();
        initLongSoundMap();
        initNumberMap();
        initCardinalMap();
        initDelimiterMap();
        initDecimalMap();
    }
    
    private static void initKanaMap() {
        KATAKANA.put("ア", "a");
        KATAKANA.put("イ", "i");
        KATAKANA.put("ウ", "u");
        KATAKANA.put("エ", "e");
        KATAKANA.put("オ", "o");
        KATAKANA.put("カ", "ka");
        KATAKANA.put("キ", "ki");
        KATAKANA.put("ク", "ku");
        KATAKANA.put("ケ", "ke");
        KATAKANA.put("コ", "ko");
        KATAKANA.put("サ", "sa");
        KATAKANA.put("シ", "shi");
        KATAKANA.put("ス", "su");
        KATAKANA.put("セ", "se");
        KATAKANA.put("ソ", "so");
        KATAKANA.put("タ", "ta");
        KATAKANA.put("チ", "chi");
        KATAKANA.put("ツ", "tsu");
        KATAKANA.put("テ", "te");
        KATAKANA.put("ト", "to");
        KATAKANA.put("ナ", "na");
        KATAKANA.put("ニ", "ni");
        KATAKANA.put("ヌ", "nu");
        KATAKANA.put("ネ", "ne");
        KATAKANA.put("ノ", "no");
        KATAKANA.put("ハ", "ha");
        KATAKANA.put("ヒ", "hi");
        KATAKANA.put("フ", "fu");
        KATAKANA.put("ヘ", "he");
        KATAKANA.put("ホ", "ho");
        KATAKANA.put("マ", "ma");
        KATAKANA.put("ミ", "mi");
        KATAKANA.put("ム", "mu");
        KATAKANA.put("メ", "me");
        KATAKANA.put("モ", "mo");
        KATAKANA.put("ヤ", "ya");
        KATAKANA.put("ユ", "yu");
        KATAKANA.put("ヨ", "yo");
        KATAKANA.put("ラ", "ra");
        KATAKANA.put("リ", "ri");
        KATAKANA.put("ル", "ru");
        KATAKANA.put("レ", "re");
        KATAKANA.put("ロ", "ro");
        KATAKANA.put("ワ", "wa");
        KATAKANA.put("ヲ", "wo");
        KATAKANA.put("ン", "n");
        KATAKANA.put("ガ", "ga");
        KATAKANA.put("ギ", "gi");
        KATAKANA.put("グ", "gu");
        KATAKANA.put("ゲ", "ge");
        KATAKANA.put("ゴ", "go");
        KATAKANA.put("ザ", "za");
        KATAKANA.put("ジ", "ji");
        KATAKANA.put("ズ", "zu");
        KATAKANA.put("ゼ", "ze");
        KATAKANA.put("ゾ", "zo");
        KATAKANA.put("ダ", "da");
        KATAKANA.put("ヂ", "ji");
        KATAKANA.put("ヅ", "zu");
        KATAKANA.put("デ", "de");
        KATAKANA.put("ド", "do");
        KATAKANA.put("バ", "ba");
        KATAKANA.put("ビ", "bi");
        KATAKANA.put("ブ", "bu");
        KATAKANA.put("ベ", "be");
        KATAKANA.put("ボ", "bo");
        KATAKANA.put("パ", "pa");
        KATAKANA.put("ピ", "pi");
        KATAKANA.put("プ", "pu");
        KATAKANA.put("ペ", "pe");
        KATAKANA.put("ポ", "po");
        KATAKANA.put("キャ", "kya");
        KATAKANA.put("キュ", "kyu");
        KATAKANA.put("キョ", "kyo");
        KATAKANA.put("シャ", "sha");
        KATAKANA.put("シュ", "shu");
        KATAKANA.put("ショ", "sho");
        KATAKANA.put("チャ", "cha");
        KATAKANA.put("チュ", "chu");
        KATAKANA.put("チョ", "cho");
        KATAKANA.put("ニャ", "nya");
        KATAKANA.put("ニュ", "nyu");
        KATAKANA.put("ニョ", "nyo");
        KATAKANA.put("ヒャ", "hya");
        KATAKANA.put("ヒュ", "hyu");
        KATAKANA.put("ヒョ", "hyo");
        KATAKANA.put("リャ", "rya");
        KATAKANA.put("リュ", "ryu");
        KATAKANA.put("リョ", "ryo");
        KATAKANA.put("ギャ", "gya");
        KATAKANA.put("ギュ", "gyu");
        KATAKANA.put("ギョ", "gyo");
        KATAKANA.put("ジャ", "ja");
        KATAKANA.put("ジュ", "ju");
        KATAKANA.put("ジョ", "jo");
        KATAKANA.put("ティ", "ti");
        KATAKANA.put("ディ", "di");
        KATAKANA.put("ツィ", "tsi");
        KATAKANA.put("ヂャ", "dya");
        KATAKANA.put("ヂュ", "dyu");
        KATAKANA.put("ヂョ", "dyo");
        KATAKANA.put("ビャ", "bya");
        KATAKANA.put("ビュ", "byu");
        KATAKANA.put("ビョ", "byo");
        KATAKANA.put("ピャ", "pya");
        KATAKANA.put("ピュ", "pyu");
        KATAKANA.put("ピョ", "pyo");
        KATAKANA.put("チェ", "che");
        KATAKANA.put("フィ", "fi");
        KATAKANA.put("フェ", "fe");
        KATAKANA.put("ウィ", "wi");
        KATAKANA.put("ウェ", "we");
        KATAKANA.put("ヴィ", "ⅴi");
        KATAKANA.put("ヴェ", "ve");
        KATAKANA.put("ー", "-");
        KATAKANA.put("「", "\"");
        KATAKANA.put("」", "\"");
        KATAKANA.put("。", ".");
    }
    
    private static void initLongSoundMap() {
        LONG_SOUND_LETTER.put('a', "ā");
        LONG_SOUND_LETTER.put('i', "ī");
        LONG_SOUND_LETTER.put('u', "ū");
        LONG_SOUND_LETTER.put('e', "ē");
        LONG_SOUND_LETTER.put('o', "ō");
    }
    
    private static void initNumberMap() {
        NUMBER.put('0',  BigDecimal.valueOf(0));
        NUMBER.put('1',  BigDecimal.valueOf(1));
        NUMBER.put('2',  BigDecimal.valueOf(2));
        NUMBER.put('3',  BigDecimal.valueOf(3));
        NUMBER.put('4',  BigDecimal.valueOf(4));
        NUMBER.put('5',  BigDecimal.valueOf(5));
        NUMBER.put('6',  BigDecimal.valueOf(6));
        NUMBER.put('7',  BigDecimal.valueOf(7));
        NUMBER.put('8',  BigDecimal.valueOf(8));
        NUMBER.put('9',  BigDecimal.valueOf(9));
        NUMBER.put('０', BigDecimal.valueOf(0));
        NUMBER.put('１', BigDecimal.valueOf(1));
        NUMBER.put('２', BigDecimal.valueOf(2));
        NUMBER.put('３', BigDecimal.valueOf(3));
        NUMBER.put('４', BigDecimal.valueOf(4));
        NUMBER.put('５', BigDecimal.valueOf(5));
        NUMBER.put('６', BigDecimal.valueOf(6));
        NUMBER.put('７', BigDecimal.valueOf(7));
        NUMBER.put('８', BigDecimal.valueOf(8));
        NUMBER.put('９', BigDecimal.valueOf(9));
        NUMBER.put('〇', BigDecimal.valueOf(0));
        NUMBER.put('一', BigDecimal.valueOf(1));
        NUMBER.put('二', BigDecimal.valueOf(2));
        NUMBER.put('三', BigDecimal.valueOf(3));
        NUMBER.put('四', BigDecimal.valueOf(4));
        NUMBER.put('五', BigDecimal.valueOf(5));
        NUMBER.put('六', BigDecimal.valueOf(6));
        NUMBER.put('七', BigDecimal.valueOf(7));
        NUMBER.put('八', BigDecimal.valueOf(8));
        NUMBER.put('九', BigDecimal.valueOf(9));
        NUMBER.put('零', BigDecimal.valueOf(0));
        NUMBER.put('壱', BigDecimal.valueOf(1));
        NUMBER.put('弐', BigDecimal.valueOf(2));
        NUMBER.put('参', BigDecimal.valueOf(3));
        NUMBER.put('肆', BigDecimal.valueOf(4));
        NUMBER.put('伍', BigDecimal.valueOf(5));
        NUMBER.put('陸', BigDecimal.valueOf(6));
        NUMBER.put('漆', BigDecimal.valueOf(7));
        NUMBER.put('捌', BigDecimal.valueOf(8));
        NUMBER.put('玖', BigDecimal.valueOf(9));
        NUMBER.put('Ⅰ', BigDecimal.valueOf(1));
        NUMBER.put('Ⅱ', BigDecimal.valueOf(2));
        NUMBER.put('Ⅲ', BigDecimal.valueOf(3));
        NUMBER.put('Ⅳ', BigDecimal.valueOf(4));
        NUMBER.put('Ⅴ', BigDecimal.valueOf(5));
        NUMBER.put('Ⅵ', BigDecimal.valueOf(6));
        NUMBER.put('Ⅶ', BigDecimal.valueOf(7));
        NUMBER.put('Ⅷ', BigDecimal.valueOf(8));
        NUMBER.put('Ⅸ', BigDecimal.valueOf(9));
        NUMBER.put('Ⅹ', BigDecimal.valueOf(10));
        NUMBER.put('Ⅺ', BigDecimal.valueOf(11));
        NUMBER.put('Ⅻ', BigDecimal.valueOf(12));
        NUMBER.put('ⅰ', BigDecimal.valueOf(1));
        NUMBER.put('ⅱ', BigDecimal.valueOf(2));
        NUMBER.put('ⅲ', BigDecimal.valueOf(3));
        NUMBER.put('ⅳ', BigDecimal.valueOf(4));
        NUMBER.put('ⅴ', BigDecimal.valueOf(5));
        NUMBER.put('ⅵ', BigDecimal.valueOf(6));
        NUMBER.put('ⅶ', BigDecimal.valueOf(7));
        NUMBER.put('ⅷ', BigDecimal.valueOf(8));
        NUMBER.put('ⅸ', BigDecimal.valueOf(9));
        NUMBER.put('ⅹ', BigDecimal.valueOf(10));
        NUMBER.put('ⅺ', BigDecimal.valueOf(11));
        NUMBER.put('ⅻ', BigDecimal.valueOf(12));
    }
    
    private static void initCardinalMap() {
        CARDINAL.put('十', BigDecimal.valueOf(10));
        CARDINAL.put('百', BigDecimal.valueOf(100));
        CARDINAL.put('千', BigDecimal.valueOf(1000));
        CARDINAL.put('万', BigDecimal.valueOf(10000));
        CARDINAL.put('億', BigDecimal.valueOf(100000000));
        CARDINAL.put('兆', BigDecimal.valueOf(1000000000000L));
        CARDINAL.put('京', BigDecimal.valueOf(10000000000000000L));
        CARDINAL.put('拾', BigDecimal.valueOf(10));
        CARDINAL.put('陌', BigDecimal.valueOf(100));
        CARDINAL.put('佰', BigDecimal.valueOf(100));
        CARDINAL.put('阡', BigDecimal.valueOf(1000));
        CARDINAL.put('仟', BigDecimal.valueOf(1000));
        CARDINAL.put('萬', BigDecimal.valueOf(10000));
        CARDINAL.put('割', BigDecimal.valueOf(0.1));
        CARDINAL.put('分', BigDecimal.valueOf(0.01));
        CARDINAL.put('厘', BigDecimal.valueOf(0.001));
        CARDINAL.put('毛', BigDecimal.valueOf(0.0001));
    }
    
    private static void initDelimiterMap() {
        DELIMITER.put(',',  "Comma");
    }
    
    private static void initDecimalMap() {
        DECIMAL.put('.', "Full Stop");
    }
    
    private JapaneseMapper() {
        throw new AssertionError();
    }
    
    public static boolean isDelimiter(final Character c) {
        return JapaneseMapper.DELIMITER.containsKey(c);
    }
    
    public static boolean isDecimalPoint(final Character c) {
        return DECIMAL.containsKey(c);
    }
    
    public static BigDecimal getNumber(final Character c) {
        return JapaneseMapper.NUMBER.get(c);
    }
    
    public static BigDecimal getCardinal(final Character c) {
        return JapaneseMapper.CARDINAL.get(c);
    }
    
    public static boolean isKatakana(final String c) {
        return JapaneseMapper.KATAKANA.containsKey(c);
    }
    
    public static String getRomaji(final String c) {
        return JapaneseMapper.KATAKANA.get(c);
    }
    
    public static boolean isLongSoundLetter(final Character c) {
        return JapaneseMapper.LONG_SOUND_LETTER.containsKey(c);
    }
    
    public static String getLetterWithDiacritic(final Character c) {
        return JapaneseMapper.LONG_SOUND_LETTER.get(c);
    }
}
