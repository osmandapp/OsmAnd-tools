package net.osmand.reviews.mangrove;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import net.osmand.data.LatLon;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static net.osmand.osm.edit.Entity.EntityType.NODE;
import static net.osmand.osm.edit.Entity.EntityType.WAY;
import static org.junit.Assert.assertEquals;

public final class OsmCodingTest {
    @Test
    public void mapCompleteReviewIsMapped() {
        testCoding(
                testReview(
                        "7dY9O8VqLmb13zRc0ilHgvESL_w7K3e5qiSP8dtGdYx31bbrkioXW3u_k_iXUasRZuFUu8wHjLwnwFkCzcqWnQ", // real signature included for reference
                        1758363120L,
                        "geo:63.3953391,13.0955955?q=%C3%85relagat&u=10",
                        "https://mapcomplete.org/food.html?z=15.8&lat=63.39572501311335&lon=13.09024621583444#node/12017528501"
                ),
                new OsmCoding.OsmPoi(new LatLon(63.3953391, 13.0955955), "Årelagat", NODE, 12017528501L)
        );
    }

    @Test
    public void mapCompleteWayIsMapped() {
        testCoding(
                testReview(
                        "GMgMhbCLnuH-CjWwqS1aeatqahiX4EtcyUFH1bP2lABYD63Uwe60HvWp0uh-tMC7BZRjGrC-bMSDH2oB2HzO4A", // real signature included for reference
                        1758239777L,
                        "geo:63.2949083,13.209174449999999?q=V%C3%A4laberget&u=26",
                        "https://mapcomplete.org/climbing.html?z=10.6&lat=63.34203572731937&lon=13.213062443537638#way/1295679398"
                ),
                new OsmCoding.OsmPoi(new LatLon(63.2949083, 13.209174449999999), "Välaberget", WAY, 1295679398L));
    }

    @Test
    public void localhostNodeIsMapped() {
        testCoding(
                testReview(
                        "jpn0IODSav_5jpAmvZg9XbBquG8niDrOYibvIZm4SR6g2pJ-_VxdZdH6m0lOMGjZOKBLvw3Z5d2ttNBtn9EoyA", // real signature included for reference
                        1758374767L,
                        "geo:38.7474422,-9.2978562?q=Tagus&u=10",
                        "https://localhost/pets.html?z=18.6&lat=38.74760329021527&lon=-9.297838124200098#node/13158036298"
                ),
                new OsmCoding.OsmPoi(new LatLon(38.7474422, -9.2978562), "Tagus", NODE, 13158036298L)
        );
    }

    @Test
    public void localhostWayIsMapped() {
        testCoding(
                testReview(
                        "DfMrExUEMMB3pumOwOslkXa9AsrCBYrPW3m3WbG25LfNaEH7VxlHfF9cE-yURxMSSOsEFz-T_Qq1g1Fno9sbYw", // real signature included for reference
                        1757515446L,
                        "geo:48.139196,11.56464365?q=Pommesfreunde&u=9",
                        "https://localhost/food.html?z=19.7&lat=48.13916747724204&lon=11.564662384220355#way/271078178"
                ),
                new OsmCoding.OsmPoi(new LatLon(48.139196, 11.56464365), "Pommesfreunde", WAY, 271078178L)
        );
    }

    @Test
    public void websiteReviewIsSkipped() {
        Review review = testReview(
                "TpFpBUUPn3M0FOYUwcjtW9DtBR12hlZsWNZQHETbDk-mHJWbxhEx8SpuPypj93Hz5eypSuXFqgparW9skUZfXQ", // real signature included for reference
                1757270171L,
                "https://retroachievements.vercel.app",
                "https://mangrove.reviews"
        );
        Map<Review, OsmCoding.OsmPoi> pois = OsmCoding.resolveOsmPois(ImmutableSet.of(review));

        assertEquals(ImmutableMap.of(), pois);
    }

    @Test
    public void movedNodeUsesMostRecentlyReviewedCoordinates() {
        Review newerReview = testReview(
                "8tx08XqXVUFngd7aO7JU2xjS2czOYyYoW-81zJTs7CcVvgpq1gtjaksdOmNiM7109aCgBvBF_V_tyHZ2OgLdag", // real signature included for reference
                1696523912L,
                "geo:51.0662649,3.7382839?q=Eco-shop&u=10",
                "https://mapcomplete.org/shops?z=12.8&lat=51.051741224340475&lon=3.75277168201535&filter-shops-shop-type=%7B%22search%22%3A%22second_hand%7Cthrift%22%7D&layer-pharmacy=false#node/10864373586"
        );
        Review olderReview = testReview(
                "ejaBX5cyEoTEszz8SPdOBT8YOV45Q5oD61MM6riyaY_BCFJ8hNCehSKCqxZhEkxkjs3rWeBdiDt4jlz8BKp0aQ", // real signature included for reference
                1683232712L,
                "geo:51.0659562,3.7384766?q=Eco-shop&u=10",
                "http://127.0.0.1:1234/theme.html?layout=shops&test=false&z=17&lat=51.06574&lon=3.73993&language=en&layer-range=false#node/10864373586"
        );
        testCoding(ImmutableSet.of(newerReview, olderReview),
                ImmutableMap.of(newerReview, new OsmCoding.OsmPoi(new LatLon(51.0662649, 3.7382839), "Eco-shop", NODE, 10864373586L)));

    }

    private static void testCoding(Review review, OsmCoding.OsmPoi expectedPoi) {
        Map<Review, OsmCoding.OsmPoi> pois = OsmCoding.resolveOsmPois(ImmutableSet.of(review));
        assertEquals(expectedPoi, pois.get(review));
    }

    private static void testCoding(Set<Review> reviews, Map<Review, OsmCoding.OsmPoi> expectedPois) {
        Map<Review, OsmCoding.OsmPoi> pois = OsmCoding.resolveOsmPois(reviews);
        assertEquals(expectedPois, pois);
    }

    private static Review testReview(String signature, long iat, String sub, String clientId) {
        return new Review(
                signature,
                "irrelevant kid",
                new Review.Payload(
                        iat,
                        sub,
                        null,
                        null,
                        null,
                        null,
                        new Review.Payload.Metadata(
                                clientId,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
                        )
                )
        );
    }
}
