package net.osmand.osm;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class RelationTagsPropagationTest {

    @Test
    public void testCompareSortValuesNumeric() {
        assertTrue(RelationTagsPropagation.compareSortValues("2", "10") < 0);
        assertTrue(RelationTagsPropagation.compareSortValues("10", "2") > 0);
        assertTrue(RelationTagsPropagation.compareSortValues("4", "4") == 0);
        assertTrue(RelationTagsPropagation.compareSortValues("7", "10") < 0);
    }

    @Test
    public void testCompareSortValuesNonNumeric() {
        assertTrue(RelationTagsPropagation.compareSortValues("administrative", "national_park") < 0);
        assertTrue(RelationTagsPropagation.compareSortValues("national_park", "administrative") > 0);
        assertTrue(RelationTagsPropagation.compareSortValues("administrative", "administrative") == 0);
    }

    @Test
    public void testCompareSortValuesMixed() {
        assertTrue(RelationTagsPropagation.compareSortValues("2", "administrative") != 0);
        assertTrue(RelationTagsPropagation.compareSortValues("administrative", "2") != 0);
        assertTrue(RelationTagsPropagation.compareSortValues("", "2") != 0);
    }
}
