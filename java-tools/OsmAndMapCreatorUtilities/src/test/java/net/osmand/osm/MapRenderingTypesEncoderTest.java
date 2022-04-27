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
        //osmc:symbol=blue:yellow:white_diamond:blue_diamond_right
        tagsList.add(Map.of(OSMC_TAG, resultTagsList.get(4).get(OSMC_TAG)));
        //osmc:symbol=blue:shell_modern
        tagsList.add(Map.of(OSMC_TAG, resultTagsList.get(5).get(OSMC_TAG)));
        //osmc:symbol=red:red_dot
        tagsList.add(Map.of(OSMC_TAG, resultTagsList.get(6).get(OSMC_TAG)));
        //osmc:symbol=red:white:green_circle:1:black
        tagsList.add(Map.of(OSMC_TAG, resultTagsList.get(7).get(OSMC_TAG)));
        //osmc:symbol=black:black:X29:white
        tagsList.add(Map.of(OSMC_TAG, resultTagsList.get(8).get(OSMC_TAG)));
        //osmc:symbol=red:white:red_bar:FW:gray
        tagsList.add(Map.of(OSMC_TAG, resultTagsList.get(9).get(OSMC_TAG)));
        //osmc:symbol=green:white:green_bar
        tagsList.add(Map.of(OSMC_TAG, resultTagsList.get(10).get(OSMC_TAG)));
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
                Map.entry("osmc:symbol", "red:red:yellow_lower:black"),
                Map.entry("osmc_symbol_red", ""),
                Map.entry("osmc_symbol", "red"),
                Map.entry("osmc_shape", "bar"),
                Map.entry("osmc_symbol_red_yellow_name", "."),
                Map.entry("osmc_waycolor", "red"),
                Map.entry("osmc_background", "red"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "yellow_lower")
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
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "blue:yellow:white_diamond:blue_diamond_right"),
                Map.entry("osmc_symbol_blue", ""),
                Map.entry("osmc_symbol", "blue"),
                Map.entry("osmc_shape", "circle"),
                Map.entry("osmc_symbol_yellow_white_name", "."),
                Map.entry("osmc_waycolor", "blue"),
                Map.entry("osmc_background", "yellow"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "white_diamond"),
                Map.entry("osmc_foreground2", "blue_diamond_right")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "blue:shell_modern"),
                Map.entry("osmc_symbol_blue", ""),
                Map.entry("osmc_symbol", "blue"),
                Map.entry("osmc_shape", "none"),
                Map.entry("osmc_symbol_shell_modern_name", "."),
                Map.entry("osmc_waycolor", "blue"),
                Map.entry("osmc_background", ""),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "shell_modern")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "red:red_dot"),
                Map.entry("osmc_symbol_red", ""),
                Map.entry("osmc_symbol", "red"),
                Map.entry("osmc_shape", "none"),
                Map.entry("osmc_symbol_red_name", "."),
                Map.entry("osmc_waycolor", "red"),
                Map.entry("osmc_background", "red_dot"),
                Map.entry("osmc_stub_name", ".")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "red:white:green_circle:1:black"),
                Map.entry("osmc_symbol_red", ""),
                Map.entry("osmc_symbol", "red"),
                Map.entry("osmc_shape", "circle"),
                Map.entry("osmc_symbol_white_green_name", "."),
                Map.entry("osmc_waycolor", "red"),
                Map.entry("osmc_background", "white"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "green_circle"),
                Map.entry("osmc_foreground2", ""),
                Map.entry("osmc_text", "1"),
                Map.entry("osmc_text_symbol", "1"),
                Map.entry("osmc_textcolor", "black")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "black:black:X29:white"),
                Map.entry("osmc_symbol_black", ""),
                Map.entry("osmc_symbol", "black"),
                Map.entry("osmc_shape", "none"),
                Map.entry("osmc_symbol_black_X29_name", "."),
                Map.entry("osmc_waycolor", "black"),
                Map.entry("osmc_background", "black"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", ""),
                Map.entry("osmc_foreground2", ""),
                Map.entry("osmc_text", "X29"),
                Map.entry("osmc_text_symbol", "X29"),
                Map.entry("osmc_textcolor", "white")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "red:white:red_bar:FW:gray"),
                Map.entry("osmc_symbol_red", ""),
                Map.entry("osmc_symbol", "red"),
                Map.entry("osmc_shape", "bar"),
                Map.entry("osmc_symbol_white_red_name", "."),
                Map.entry("osmc_waycolor", "red"),
                Map.entry("osmc_background", "white"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "red_bar"),
                Map.entry("osmc_foreground2", ""),
                Map.entry("osmc_text", "FW"),
                Map.entry("osmc_text_symbol", "FW"),
                Map.entry("osmc_textcolor", "gray")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "green:white:green_bar"),
                Map.entry("osmc_symbol_green", ""),
                Map.entry("osmc_symbol", "green"),
                Map.entry("osmc_shape", "bar"),
                Map.entry("osmc_symbol_white_green_name", "."),
                Map.entry("osmc_waycolor", "green"),
                Map.entry("osmc_background", "white"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "green_bar")
        ));
    }
}