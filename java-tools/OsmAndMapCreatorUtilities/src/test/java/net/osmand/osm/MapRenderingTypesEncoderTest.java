package net.osmand.osm;

import org.junit.Before;
import org.junit.Test;

import net.osmand.osm.edit.Entity;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
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

    @Test
    public void testIntegrityScoresSurfaceAfterTagTransform() {
        // rendering_types.xml renames these values, the score must use the new value
        assertSameIntegrity("soil", "ground");
        assertSameIntegrity("grit", "gravel");
        assertSameIntegrity("hard", "compacted");
        assertSameIntegrity("bare_rock", "stone");
        assertSameIntegrity("woodchips", "wood");
        assertSameIntegrity("dirt/sand", "dirt");
        assertSameIntegrity("moss", "grass");
        assertSameIntegrity("grass;earth", "grass;ground");
    }

    @Test
    public void testIntegrityKeepsOtherTagsAsInOsm() {
        // only surface is taken after tag_transform: highway=path is not scored as footway in routing,
        // smoothness=very_good is not scored as excellent
        assertEquals("4", integrity(ROUTING, "highway", "path", "foot", "designated")[1]);
        assertEquals("0", integrity(ROUTING, "highway", "track", "surface", "compacted", "smoothness", "very_good")[1]);
    }

    @Test
    public void testIntegrityOfUnpavedSurfaces() {
        assertSameIntegrity("laterite", "ground");
        assertSameIntegrity("clay", "ground");
        assertSameIntegrity("grass;ground", "grass");
        assertSameIntegrity("ground;grass", "grass");
        assertSameIntegrity("gravel;ground", "gravel");
        assertSameIntegrity("asphalt;mud", "mud");
        assertEquals("0", integrity(MAP, "highway", "unclassified", "surface", "asphalt")[0]);
        assertEquals("3", integrity(MAP, "highway", "unclassified", "surface", "ground")[0]);
    }

    private static final MapRenderingTypesEncoder.EntityConvertApplyType MAP = MapRenderingTypesEncoder.EntityConvertApplyType.MAP;
    private static final MapRenderingTypesEncoder.EntityConvertApplyType ROUTING = MapRenderingTypesEncoder.EntityConvertApplyType.ROUTING;

    private void assertSameIntegrity(String surface, String expectedLike) {
        for (MapRenderingTypesEncoder.EntityConvertApplyType appType : List.of(MAP, ROUTING)) {
            assertArrayEquals(surface + " in " + appType,
                    integrity(appType, "highway", "unclassified", "surface", expectedLike),
                    integrity(appType, "highway", "unclassified", "surface", surface));
        }
    }

    // osmand_highway_integrity and osmand_highway_integrity_brouting after transformTags
    private String[] integrity(MapRenderingTypesEncoder.EntityConvertApplyType appType, String... tagValues) {
        Map<String, String> tags = new LinkedHashMap<>();
        for (int i = 0; i < tagValues.length; i += 2) {
            tags.put(tagValues[i], tagValues[i + 1]);
        }
        Map<String, String> res = mapRenderingTypesEncoder.transformTags(tags, Entity.EntityType.WAY, appType);
        return new String[] { res.get("osmand_highway_integrity"), res.get("osmand_highway_integrity_brouting") };
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
        //osmc:symbol=red:white: :V:red
        tagsList.add(Map.of(OSMC_TAG, resultTagsList.get(11).get(OSMC_TAG)));
        //osmc:symbol=yellow::yellow_bar:PR: https://www.openstreetmap.org/relation/12548505
        tagsList.add(Map.of(OSMC_TAG, resultTagsList.get(12).get(OSMC_TAG)));
    }

    private void createTransformOsmcResult() {
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "yellow:brown::HSG:gray"),
                Map.entry("osmc_waycolor", "yellow"),
                Map.entry("osmc_background", "brown_3"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_text", "HSG"),
                Map.entry("osmc_textcolor", "gray")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "purple:white:::M:purple"),
                Map.entry("osmc_waycolor", "purple"),
                Map.entry("osmc_background", "white"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_text", "M"),
                Map.entry("osmc_textcolor", "purple")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "red:red:yellow_lower:black"),
                Map.entry("osmc_waycolor", "red"),
                Map.entry("osmc_background", "red"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "yellow_lower")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "white:red:white_turned_T"),
                Map.entry("osmc_waycolor", "white"),
                Map.entry("osmc_background", "red"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "white_turned_t")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "blue:yellow:white_diamond:blue_diamond_right"),
                Map.entry("osmc_waycolor", "blue"),
                Map.entry("osmc_background", "yellow"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "white_diamond"),
                Map.entry("osmc_foreground2", "blue_diamond_right")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "blue:shell_modern"),
                Map.entry("osmc_waycolor", "blue"),
                Map.entry("osmc_background", "blue"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "shell_modern")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "red:red_dot"),
                Map.entry("osmc_waycolor", "red"),
                Map.entry("osmc_background", "red_dot"),
                Map.entry("osmc_stub_name", ".")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "red:white:green_circle:1:black"),
                Map.entry("osmc_waycolor", "red"),
                Map.entry("osmc_background", "white"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "green_circle"),
                Map.entry("osmc_text", "1"),
                Map.entry("osmc_textcolor", "black")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "black:black:X29:white"),
                Map.entry("osmc_waycolor", "black"),
                Map.entry("osmc_background", "black_3"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_text", "X29"),
                Map.entry("osmc_textcolor", "white")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "red:white:red_bar:FW:gray"),
                Map.entry("osmc_waycolor", "red"),
                Map.entry("osmc_background", "white_2"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "red_bar_2"),
                Map.entry("osmc_text", "FW"),
                Map.entry("osmc_textcolor", "gray")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "green:white:green_bar"),
                Map.entry("osmc_waycolor", "green"),
                Map.entry("osmc_background", "white"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "green_bar")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "red:white: :V:red"),
                Map.entry("osmc_waycolor", "red"),
                Map.entry("osmc_background", "white"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_text", "V"),
                Map.entry("osmc_textcolor", "red")
        ));
        resultTagsList.add(Map.ofEntries(
                Map.entry("osmc:symbol", "yellow::yellow_bar:PR:"),
                Map.entry("osmc_waycolor", "yellow"),
                Map.entry("osmc_background", "white_2"),
                Map.entry("osmc_stub_name", "."),
                Map.entry("osmc_foreground", "yellow_bar_2"),
                Map.entry("osmc_text", "PR")
        ));
    }
}
