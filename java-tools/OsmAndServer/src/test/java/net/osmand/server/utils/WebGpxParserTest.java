package net.osmand.server.utils;

import static net.osmand.server.utils.WebGpxTestData.load;
import static net.osmand.server.utils.WebGpxTestData.sentBackJson;
import static net.osmand.server.utils.WebGpxTestData.trackData;
import static net.osmand.server.utils.WebGpxTestData.trackDataJson;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.Route;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import okio.Buffer;

// WebGpxParser: GPX to the track data of the web map and back (src/test/resources/gpx/points-with-extensions.gpx)
public class WebGpxParserTest {

	@Test
	public void wptFieldsMovedOutOfExtensions() throws IOException {
		List<WebGpxParser.Wpt> wpts = trackData().wpts;
		assertEquals(3, wpts.size());

		WebGpxParser.Wpt starbucks = wpts.get(0);
		assertEquals("Starbucks", starbucks.name);
		assertEquals("Cafe", starbucks.category);
		assertEquals(52.5194036, starbucks.lat, 0);
		assertEquals(13.3887345, starbucks.lon, 0);
		assertEquals("#a71de1", starbucks.color);
		assertEquals("amenity_cafe", starbucks.icon);
		assertEquals("circle", starbucks.background);
		assertEquals("Friedrichstraße (Spandau), Berlin", starbucks.address);
		assertEquals(Map.of("amenity_opening_hours", "Mo-Fr 07:00-21:00; Sa 09:00-21:00; Su 09:00-19:30",
				"osm_tag_brand", "Starbucks"), starbucks.ext.getExtensions());

		WebGpxParser.Wpt test = wpts.get(1);
		assertEquals("true", test.hidden);
		assertEquals(Map.of("test:country", "United States"), test.ext.getExtensions());

		WebGpxParser.Wpt velo = wpts.get(2);
		assertEquals("6/13/2016 9:32:27 PM", velo.desc);
		assertNull(velo.category);
		assertNull(velo.ext.getExtensions());
	}

	@Test
	public void pointsGroups() throws IOException {
		Map<String, WebGpxParser.WebPointsGroup> groups = trackData().pointsGroups;
		assertEquals(List.of("Cafe", "SOTM", ""), List.copyOf(groups.keySet()));

		WebGpxParser.WebPointsGroup cafe = groups.get("Cafe");
		assertEquals("#a71de1", cafe.color);
		assertEquals("special_star", cafe.iconName);
		assertEquals("circle", cafe.backgroundType);
		assertFalse(cafe.hidden);

		WebGpxParser.WebPointsGroup sotm = groups.get("SOTM");
		assertEquals("place_town", sotm.iconName);
		assertEquals("circle", sotm.backgroundType);
		assertTrue(sotm.hidden);
	}

	// two routes over two segments: a route point per rtept, the trkpts of a leg in its geometry
	@Test
	public void routePointsWithSegmentGeometry() throws IOException {
		WebGpxParser.TrackData data = trackData();
		assertEquals(1, data.tracks.size());
		List<WebGpxParser.Point> points = points(data.tracks.get(0));
		assertEquals(4, points.size());

		assertEquals("pedestrian", points.get(0).profile);
		assertTrue(points.get(0).geometry.isEmpty());
		assertEquals(List.of("94", "95", "96"), hr(points.get(1).geometry));
		// the last point of a segment that is not the last one ends it with a gap
		assertEquals(GpxUtilities.GAP_PROFILE_TYPE, points.get(1).profile);

		assertEquals("bicycle", points.get(2).profile);
		assertTrue(points.get(2).geometry.isEmpty());
		assertEquals("bicycle", points.get(3).profile);
		assertEquals(List.of("95", "91", "91"), hr(points.get(3).geometry));
		assertEquals(Map.of("displaycolor", "Red"), track(data.tracks.get(0)).getExtensions());
	}

	@Test
	public void savedWptsAsLoaded() throws IOException {
		List<WptPt> before = load().getPointsList();
		List<WptPt> after = save().getPointsList();
		assertEquals(before.size(), after.size());
		for (int i = 0; i < before.size(); i++) {
			WptPt b = before.get(i), a = after.get(i);
			assertEquals(b.getName(), a.getName());
			assertEquals(b.getDesc(), a.getDesc());
			assertEquals(b.getCategory(), a.getCategory());
			assertEquals(b.getLat(), a.getLat(), 0);
			assertEquals(b.getLon(), a.getLon(), 0);
			assertEquals(b.getColor(0), a.getColor(0));
			assertEquals(withoutColor(b.getExtensions()), withoutColor(a.getExtensions()));
		}
	}

	@Test
	public void savedGroupsAsLoaded() throws IOException {
		Map<String, GpxUtilities.PointsGroup> before = load().getPointsGroups();
		Map<String, GpxUtilities.PointsGroup> after = save().getPointsGroups();
		assertEquals(before.keySet(), after.keySet());
		for (String name : before.keySet()) {
			assertEquals(name, before.get(name).getColor(), after.get(name).getColor());
			assertEquals(name, before.get(name).getIconName(), after.get(name).getIconName());
			assertEquals(name, before.get(name).getBackgroundType(), after.get(name).getBackgroundType());
			assertEquals(name, before.get(name).getHidden(), after.get(name).getHidden());
		}
	}

	@Test
	public void savedSegmentsAsLoaded() throws IOException {
		List<TrkSegment> before = segments(load());
		List<TrkSegment> after = segments(save());
		assertEquals(2, after.size());
		for (int s = 0; s < before.size(); s++) {
			List<WptPt> b = before.get(s).getPoints(), a = after.get(s).getPoints();
			assertEquals(b.size(), a.size());
			for (int i = 0; i < b.size(); i++) {
				assertEquals(b.get(i).getLat(), a.get(i).getLat(), 0);
				assertEquals(b.get(i).getLon(), a.get(i).getLon(), 0);
				assertEquals(b.get(i).getEle(), a.get(i).getEle(), 0);
				assertEquals(b.get(i).getTime(), a.get(i).getTime());
				assertEquals(b.get(i).getExtensions(), a.get(i).getExtensions());
			}
		}
	}

	@Test
	public void savedRoutesKeepPoints() throws IOException {
		List<Route> before = load().getRoutes();
		List<Route> after = save().getRoutes();
		assertEquals(before.size(), after.size());
		for (int r = 0; r < before.size(); r++) {
			List<WptPt> b = before.get(r).getPoints(), a = after.get(r).getPoints();
			assertEquals(b.size(), a.size());
			for (int i = 0; i < b.size(); i++) {
				assertEquals(b.get(i).getLat(), a.get(i).getLat(), 0);
				assertEquals(b.get(i).getLon(), a.get(i).getLon(), 0);
				assertEquals(r + ":" + i, b.get(i).getTrkPtIndex(), a.get(i).getTrkPtIndex());
			}
			assertEquals(b.get(0).getExtensions(), a.get(0).getExtensions());
		}
		assertEquals(before.get(1).getPoints().get(1).getExtensions(), after.get(1).getPoints().get(1).getExtensions());
		// the end of a segment followed by another one gets the gap profile, which a rtept does not write (as the app)
		assertNull(after.get(0).getPoints().get(1).getProfileType());
	}

	// the appearance of a track without color and width, as GpxService sends it
	@Test
	public void savedWithAppearanceWithoutColor() throws IOException {
		WebGpxParser.TrackData data = GpxJson.create().fromJson(trackDataJson(GpxJson.createWithNans()),
				WebGpxParser.TrackData.class);
		assertNull(data.trackAppearance.color);
		GpxFile gpxFile = new WebGpxParser().createGpxFileFromTrackData(data);
		assertNull(GpxUtilities.INSTANCE.writeGpx(null, new Buffer(), gpxFile, null));
		assertFalse(gpxFile.getExtensions().containsKey(WebGpxParser.GPX_EXT_COLOR));
	}

	// as /gpx/save-track-data gets the track from the web map, then loaded as the apps load the saved file
	private static GpxFile save() throws IOException {
		WebGpxParser.TrackData data = GpxJson.create().fromJson(sentBackJson(GpxJson.createWithNans()),
				WebGpxParser.TrackData.class);
		Buffer out = new Buffer();
		GpxUtilities.INSTANCE.writeGpx(null, out, new WebGpxParser().createGpxFileFromTrackData(data), null);

		return GpxUtilities.INSTANCE.loadGpxFile(out);
	}

	// a point color equal to its group color is not written, the loaded point takes the group color: compared by getColor
	private static Map<String, String> withoutColor(Map<String, String> extensions) {
		Map<String, String> res = extensions == null ? new HashMap<>() : new HashMap<>(extensions);
		res.remove(GpxUtilities.COLOR_NAME_EXTENSION);

		return res;
	}

	// without the general track the parser adds
	private static List<TrkSegment> segments(GpxFile gpxFile) {
		return gpxFile.getTracks().stream().filter(t -> !t.getGeneralTrack()).flatMap(t -> t.getSegments().stream()).toList();
	}

	@SuppressWarnings("unchecked")
	private static List<WebGpxParser.Point> points(WebGpxParser.WebTrack track) {
		return (List<WebGpxParser.Point>) ReflectionTestUtils.getField(track, "points");
	}

	private static Track track(WebGpxParser.WebTrack track) {
		return (Track) ReflectionTestUtils.getField(track, "ext");
	}

	private static List<String> hr(List<WebGpxParser.Point> geometry) {
		return geometry.stream().map(p -> p.ext.getExtensions().get("gpxtpx:hr")).toList();
	}
}
