package net.osmand.reviews.mangrove;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GeoUriTest {
    @Test
    public void parsesUrlEncodedLatLonUnc() {
        assertEquals(new GeoUri(63.3953391, 13.0955955, 10.0, ImmutableMap.of()),
                GeoUri.parseUrlEncoded("geo:63.3953391,13.0955955?u=10"));
    }

    @Test
    public void parsesUrlEncodedQuery() {
        assertEquals(new GeoUri(0, 0, null, ImmutableMap.of("q", "The query")),
                GeoUri.parseUrlEncoded("geo:0,0?q=The%20query"));
    }

    @Test
    public void parsesUrlEncodedQueryUtf8() {
        assertEquals(new GeoUri(0, 0, null, ImmutableMap.of("q", "Välaberget")),
                GeoUri.parseUrlEncoded("geo:0,0?q=V%C3%A4laberget"));
    }

    @Test
    public void parsesUrlEncodedQueryWithAmpersands() {
        assertEquals(new GeoUri(0, 0, null, ImmutableMap.of("q", "Casper East RV Park & Campground")),
                GeoUri.parseUrlEncoded("geo:0,0?q=Casper%20East%20RV%20Park%20%26%20Campground"));
    }
}
