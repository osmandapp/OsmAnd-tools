package net.osmand.osm;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapRenderingTypesEncoderTest {
    
    private MapRenderingTypesEncoder mapRenderingTypesEncoder;
    List<Map<String, String>> tagsList = new ArrayList<>();
    List<Map<String, String>> resultTagsList = new ArrayList<>();
    
    @Before
    public void setUp() {
        this.mapRenderingTypesEncoder = new MapRenderingTypesEncoder("");
        createTransformOsmcResult();
        createTransformOsmcCaseList();
    }
    
    @Test
    public void testTransformOsmcAndColorTags() {
        Map<String, String> actual;
        for (Map<String, String> tags : tagsList) {
            actual = this.mapRenderingTypesEncoder.transformOsmcAndColorTags(tags);
            assertEquals(actual, resultTagsList.get(tagsList.indexOf(tags)));
        }
    }
    
    private void createTransformOsmcCaseList() {
        String OSMC_TAG = "osmc:symbol";
        
        // osmc:symbol=yellow:brown::HSG:gray
        tagsList.add(Map.of(OSMC_TAG, resultTagsList.get(0).get(OSMC_TAG)));
        // osmc:symbol=purple:white:::M:purple
        tagsList.add(Map.of(OSMC_TAG, resultTagsList.get(1).get(OSMC_TAG)));
        //osmc:symbol=red:red:yellow_lower:black
        tagsList.add(Map.of(OSMC_TAG, resultTagsList.get(2).get(OSMC_TAG)));
        //osmc:symbol=white:red:white_turned_T
        tagsList.add(Map.of(OSMC_TAG, resultTagsList.get(3).get(OSMC_TAG)));
    }
    
    private void createTransformOsmcResult() {
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "yellow:brown::HSG:gray"),
                Map.entry("osmc_symbol_yellow", ""),
                Map.entry("osmc_symbol", "yellow"),
                Map.entry("osmc_shape", "none"),
                Map.entry("osmc_symbol_red__name", "."),
                Map.entry("osmc_waycolor", "yellow"),
                Map.entry("osmc_background", "brown"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", ""),
                Map.entry("osmc_foreground2", ""),
                Map.entry("osmc_text", "HSG"),
                Map.entry("osmc_text_symbol", "HSG"),
                Map.entry("osmc_textcolor", "gray")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "purple:white:::M:purple"),
                Map.entry("osmc_symbol_blue", ""),
                Map.entry("osmc_symbol", "blue"),
                Map.entry("osmc_shape", "none"),
                Map.entry("osmc_symbol_white__name", "."),
                Map.entry("osmc_waycolor", "purple"),
                Map.entry("osmc_background", "white"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", ""),
                Map.entry("osmc_foreground2", ""),
                Map.entry("osmc_text", "M"),
                Map.entry("osmc_text_symbol", "M"),
                Map.entry("osmc_textcolor", "purple")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "red:red:yellow_lower:black")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "white:red:white_turned_T"),
                Map.entry("osmc_symbol_white", ""),
                Map.entry("osmc_symbol", "white"),
                Map.entry("osmc_shape", "bar"),
                Map.entry("osmc_symbol_red_white_name", "."),
                Map.entry("osmc_waycolor", "white"),
                Map.entry("osmc_background", "red"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "white_turned_t")
        ));
    }
}