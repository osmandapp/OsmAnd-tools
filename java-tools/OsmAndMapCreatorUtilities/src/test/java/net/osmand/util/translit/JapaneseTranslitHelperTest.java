package net.osmand.util.translit;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class JapaneseTranslitHelperTest {
    Map<String, String> testData = new HashMap<>();
    
    @Before
    public void setUp() {
        testData.put("堀田通七丁目", "Horitatōri 7 Chōme");
        testData.put("東郊通２", "Tōkōtōri 2");
        testData.put("柳ケ枝町二丁目", "Yanagae Machi 2 Chōme");
        testData.put("熱田水処理センター", "Atsuta Mizu Shori Sentā");
        testData.put("瑞穂巡回(左回り)", "Mizuho Junkai ( Hidari Mawari )");
        testData.put("名古屋高速道路小牧-大高線高架路", "Nagoya Kōsoku Dōro Omaki - Dai Daka Sen Kōka Ro");
        testData.put("松田橋", "Matsuda Kyō");
        testData.put("スガノ皮膚科", "Suga ノHifu Ka");
        testData.put("東郊通百五十一", "Tōkōtōri 151");
        testData.put("東郊通151", "Tōkōtōri 151");
        testData.put("東郊通百五十一-百五十一", "Tōkōtōri 151 - 151");
        testData.put("東郊通百五十一松田", "Tōkōtōri 151 Matsuda");
    }
    
    @Test
    public void getEnglishTransliteration() {
        for (Map.Entry<String, String> entry : testData.entrySet()) {
            String res = JapaneseTranslitHelper.getEnglishTransliteration(entry.getKey());
            assertEquals(entry.getValue(), res);
        }
    }
}