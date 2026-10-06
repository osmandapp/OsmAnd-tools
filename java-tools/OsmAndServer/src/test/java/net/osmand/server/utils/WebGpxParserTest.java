package net.osmand.server.utils;

import static net.osmand.server.utils.WebGpxTestData.load;
import static net.osmand.server.utils.WebGpxTestData.sentBackJson;
import static net.osmand.server.utils.WebGpxTestData.trackData;
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

		WebGpxParser.Wpt cafe = wpts.get(0);
		assertEquals("Cafe", cafe.name);
		assertEquals("d", cafe.desc);
		assertEquals("Food", cafe.category);
		assertEquals(50.45, cafe.lat, 0);
		assertEquals(30.52, cafe.lon, 0);
		assertEquals("#ff0000", cafe.color);
		assertEquals("cafe", cafe.icon);
		assertEquals("circle", cafe.background);
		assertEquals("Khreshchatyk 1", cafe.address);
		assertEquals(Map.of("amenity_opening_hours", "Mo-Fr 08:00-20:00", "osm_tag_phone", "+380441234567",
				"my_note", "hello"), cafe.ext.getExtensions());

		WebGpxParser.Wpt viewpoint = wpts.get(1);
		assertEquals("true", viewpoint.hidden);
		assertEquals(Map.of("test:country", "UA"), viewpoint.ext.getExtensions());

		WebGpxParser.Wpt spring = wpts.get(2);
		assertEquals("water", spring.desc);
		assertNull(spring.category);
		assertNull(spring.ext.getExtensions());
	}

	@Test
	public void pointsGroups() throws IOException {
		Map<String, WebGpxParser.WebPointsGroup> groups = trackData().pointsGroups;
		assertEquals(List.of("Food", "Nature", ""), List.copyOf(groups.keySet()));

		WebGpxParser.WebPointsGroup food = groups.get("Food");
		assertEquals("#ff0000", food.color);
		assertEquals("cafe", food.iconName);
		assertEquals("circle", food.backgroundType);
		assertFalse(food.hidden);

		WebGpxParser.WebPointsGroup nature = groups.get("Nature");
		assertEquals("special_star", nature.iconName);
		assertEquals("octagon", nature.backgroundType);
		assertTrue(nature.hidden);
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
		assertEquals(List.of("p0", "p1", "p2"), customPt(points.get(1).geometry));
		// the last point of a segment that is not the last one ends it with a gap
		assertEquals(GpxUtilities.GAP_PROFILE_TYPE, points.get(1).profile);

		assertEquals("bicycle", points.get(2).profile);
		assertTrue(points.get(2).geometry.isEmpty());
		assertEquals("bicycle", points.get(3).profile);
		assertEquals(List.of("p3", "p4", "p5"), customPt(points.get(3).geometry));
		assertEquals(Map.of("custom_trk", "x"), track(data.tracks.get(0)).getExtensions());
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
	public void savedGroupsKeepNameColorHidden() throws IOException {
		Map<String, GpxUtilities.PointsGroup> before = load().getPointsGroups();
		Map<String, GpxUtilities.PointsGroup> after = save().getPointsGroups();
		assertEquals(before.keySet(), after.keySet());
		for (String name : before.keySet()) {
			assertEquals(name, before.get(name).getColor(), after.get(name).getColor());
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
			}
			assertEquals(b.get(0).getExtensions(), a.get(0).getExtensions());
		}
		assertEquals(before.get(1).getPoints().get(1).getExtensions(), after.get(1).getPoints().get(1).getExtensions());
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

	private static List<String> customPt(List<WebGpxParser.Point> geometry) {
		return geometry.stream().map(p -> p.ext.getExtensions().get("custom_pt")).toList();
	}
}
