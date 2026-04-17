package net.osmand.server.utils;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.garmin.fit.ActivityMesg;
import com.garmin.fit.DateTime;
import com.garmin.fit.Decode;
import com.garmin.fit.Event;
import com.garmin.fit.EventType;
import com.garmin.fit.FileIdMesg;
import com.garmin.fit.Fit;
import com.garmin.fit.GarminProduct;
import com.garmin.fit.Manufacturer;
import com.garmin.fit.MesgBroadcaster;
import com.garmin.fit.RecordMesg;
import com.garmin.fit.RecordMesgListener;
import com.garmin.fit.SessionMesg;
import com.garmin.fit.Sport;

import jakarta.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.osmand.shared.KException;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.io.KFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.PointAttributes;
import net.osmand.shared.gpx.RouteActivityHelper;
import net.osmand.shared.gpx.primitives.Author;
import net.osmand.shared.gpx.primitives.RouteActivity;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;

public final class GarminFitToGpxParser {

	private static final Log LOG = LogFactory.getLog(GarminFitToGpxParser.class);

	private static final String OSMAND_FIT_TO_GPX_V1 = "OsmAndFitToGpxV1";
	private static final String GPX_AUTHOR_BRAND_GARMIN = "Garmin";

	private GarminFitToGpxParser() {
	}

	private record FitData(List<TrkSegment> segments, List<WptPt> allPoints, List<RecordMesg> fitRecords,
	                       Sport sport, long sessionStartMs, long fileCreatedMs, String sportProfileName,
	                       @Nullable Integer fileIdGarminProduct) {
	}

	/** Converts raw FIT binary data to a GpxFile. Returns null if input is empty or corrupted. */
	public static GpxFile fromFitBytes(@Nullable byte[] fitBytes, String preferredName) {
		if (fitBytes == null || fitBytes.length == 0) {
			return null;
		}
		FitData data = decodeFit(fitBytes);
		if (data == null) {
			return null;
		}
		return buildGpxFile(data, preferredName);
	}

	/** Decodes FIT binary using Garmin SDK, extracts points, segments, session metadata. */
	private static FitData decodeFit(byte[] fitBytes) {
		List<TrkSegment> segments = new ArrayList<>();
		List<WptPt> current = new ArrayList<>();
		List<RecordMesg> fitRecords = new ArrayList<>();
		List<WptPt> allPoints = new ArrayList<>();
		final Sport[] sessionSport = { null };
		final long[] sessionStartMs = { 0L };
		final long[] fileCreatedMs = { 0L };
		final String[] sportProfileName = { null };
		final Integer[] fileIdGarminProduct = { null };

		Decode decode = new Decode();
		MesgBroadcaster broadcaster = new MesgBroadcaster(decode);
		broadcaster.addListener((FileIdMesg m) -> {
			DateTime tc = m.getTimeCreated();
			if (tc != null) {
				fileCreatedMs[0] = tc.getDate().getTime();
			}
			Integer gp = garminProductFromFileId(m);
			if (gp != null) {
				fileIdGarminProduct[0] = gp;
			}
		});
		broadcaster.addListener((SessionMesg m) -> {
			Sport sp = m.getSport();
			if (sp != null && sp != Sport.INVALID) {
				sessionSport[0] = sp;
			}
			DateTime st = m.getStartTime();
			if (st != null) {
				sessionStartMs[0] = st.getDate().getTime();
			}
			String profile = m.getSportProfileName();
			if (profile != null && !profile.isBlank()) {
				sportProfileName[0] = profile.trim();
			}
			if (m.getEvent() == Event.SESSION && isStopLikeEventType(m.getEventType())) {
				flushCurrent(segments, current, null);
			}
		});
		broadcaster.addListener((ActivityMesg m) -> {
			if (m.getEvent() == Event.ACTIVITY && isStopLikeEventType(m.getEventType())) {
				flushCurrent(segments, current, null);
			}
		});
		// Splits on FIT session/activity end only (Garmin Connect–style single trkseg per activity).
		// No TIMER stop / lap splits — see Garmin GPX export behavior.
		broadcaster.addListener((RecordMesgListener) mesg -> {
			WptPt p = recordToWpt(mesg);
			if (p != null) {
				current.add(p);
				fitRecords.add(mesg);
				allPoints.add(p);
			}
		});

		try {
			broadcaster.run(new ByteArrayInputStream(fitBytes));
		} catch (Exception e) {
			LOG.warn("FIT decode failed: " + e.getMessage(), e);
			return null;
		}
		flushCurrent(segments, current, null);

		return new FitData(segments, allPoints, fitRecords,
				sessionSport[0], sessionStartMs[0], fileCreatedMs[0], sportProfileName[0], fileIdGarminProduct[0]);
	}

	/** Builds GpxFile from decoded FIT data: applies sensors, sets metadata, assembles track. */
	private static GpxFile buildGpxFile(FitData data, String preferredName) {
		// 1. Apply sensor extensions
		for (int i = 0; i < data.allPoints.size(); i++) {
			copyStandardTrackPointSensors(data.allPoints.get(i), data.fitRecords.get(i), data.sport);
		}

		// 2. Collect time range and elevation presence
		boolean anyEleFound = false;
		long firstTime = 0L;
		long lastTime = 0L;
		for (TrkSegment seg : data.segments) {
			for (WptPt p : seg.getPoints()) {
				if (!Double.isNaN(p.getEle())) {
					anyEleFound = true;
				}
				if (p.getTime() > 0L) {
					if (firstTime == 0L) {
						firstTime = p.getTime();
					}
					if (p.getTime() > lastTime) {
						lastTime = p.getTime();
					}
				}
			}
		}

		// 3. Fill GPX metadata (author, time, name, activity)
		GpxFile gpx = new GpxFile(OSMAND_FIT_TO_GPX_V1);
		gpx.setHasAltitude(anyEleFound);
		long metaTs = data.fileCreatedMs > 0L ? data.fileCreatedMs : data.sessionStartMs;
		if (metaTs <= 0L) {
			metaTs = firstTime;
		}
		if (metaTs <= 0L) {
			metaTs = System.currentTimeMillis();
		}
		gpx.getMetadata().setTime(metaTs);
		Author author = new Author();
		author.setName(gpxAuthorDeviceName(data.fileIdGarminProduct));
		gpx.getMetadata().setAuthor(author);
		String name = preferredName != null && !preferredName.isBlank() ? preferredName.trim() : null;
		if (name == null && data.sportProfileName != null) {
			name = data.sportProfileName;
		}
		if (name != null) {
			gpx.getMetadata().setName(name);
		}
		RouteActivity routeActivity = routeActivityFromFitSport(data.sport);
		if (routeActivity != null) {
			gpx.getMetadata().setRouteActivity(routeActivity);
		}
		long modTs = lastTime > 0L ? lastTime : metaTs;
		gpx.setModifiedTime(modTs);

		// 4. Assemble track from segments
		Track track = new Track();
		if (name != null) {
			track.setName(name);
		}
		for (TrkSegment seg : data.segments) {
			if (!seg.getPoints().isEmpty()) {
				track.getSegments().add(seg);
			}
		}
		if (!track.getSegments().isEmpty()) {
			gpx.getTracks().add(track);
		}
		return gpx;
	}

	private static final String DEFAULT_TEST_FIT = "/.../18872521110_ACTIVITY.fit";

	public static void main(String[] args) throws Exception {
		String fitPath = args.length > 0 && args[0] != null && !args[0].isBlank()
				? args[0].trim() : DEFAULT_TEST_FIT;
		Path fit = Paths.get(fitPath);
		if (!Files.isRegularFile(fit)) {
			LOG.error("Not a file: " + fit.toAbsolutePath());
			System.exit(1);
		}
		byte[] raw = Files.readAllBytes(fit);
		String stem = fit.getFileName().toString();
		int dot = stem.lastIndexOf('.');
		if (dot > 0) {
			stem = stem.substring(0, dot);
		}
		GpxFile gpx = fromFitBytes(raw, stem);
		if (gpx == null) {
			LOG.error("Parse failed (empty input)");
			System.exit(2);
		}
		Path out = fit.resolveSibling(stem + ".gpx");
		KException err = GpxUtilities.INSTANCE.writeGpxFile(new KFile(out.toAbsolutePath().toString()), gpx);
		if (err != null) {
			LOG.error("Write failed: " + err.getMessage());
			System.exit(3);
		}
		System.out.println(out.toAbsolutePath());
	}

	// ---- FIT field helpers ----

	private static double semicircleToDegrees(int semicircle) {
		return semicircle * (180.0 / (1L << 31));
	}

	private static boolean validCoord(Integer latSem, Integer lonSem) {
		if (latSem == null || lonSem == null) {
			return false;
		}
		if (latSem.equals(Fit.SINT32_INVALID) || lonSem.equals(Fit.SINT32_INVALID)) {
			return false;
		}
		double lat = semicircleToDegrees(latSem);
		double lon = semicircleToDegrees(lonSem);
		return lat >= -90.0 && lat <= 90.0 && lon >= -180.0 && lon <= 180.0;
	}

	private static boolean usableFloat(Float v) {
		if (v == null || v.isNaN()) {
			return false;
		}
		return Float.floatToIntBits(v) != Float.floatToIntBits(Fit.FLOAT32_INVALID);
	}

	private static boolean usableUint8Short(Short v) {
		return v != null && !v.equals(Fit.UINT8_INVALID);
	}

	private static boolean usableSint8(Byte v) {
		return v != null && !v.equals(Fit.SINT8_INVALID);
	}

	private static boolean usableUInt16(Integer v) {
		return v != null && !v.equals(Fit.UINT16_INVALID);
	}

	private static boolean isStopLikeEventType(@Nullable EventType et) {
		return et == EventType.STOP || et == EventType.STOP_ALL || et == EventType.STOP_DISABLE_ALL;
	}

	private static void flushCurrent(List<TrkSegment> out, List<WptPt> cur, String segmentName) {
		if (cur.isEmpty()) {
			return;
		}
		TrkSegment seg = new TrkSegment();
		seg.getPoints().addAll(cur);
		if (segmentName != null) {
			seg.setName(segmentName);
		}
		out.add(seg);
		cur.clear();
	}

	private static WptPt recordToWpt(RecordMesg mesg) {
		Integer latS = mesg.getPositionLat();
		Integer lonS = mesg.getPositionLong();
		if (!validCoord(latS, lonS)) {
			return null;
		}
		DateTime ts = mesg.getTimestamp();
		if (ts == null) {
			return null;
		}
		WptPt p = new WptPt();
		p.setLat(semicircleToDegrees(latS));
		p.setLon(semicircleToDegrees(lonS));
		p.setTime(ts.getDate().getTime());
		Float ele = mesg.getEnhancedAltitude();
		if (!usableFloat(ele)) {
			ele = mesg.getAltitude();
		}
		if (usableFloat(ele)) {
			p.setEle(ele.doubleValue());
		}
		if (usableUint8Short(mesg.getGpsAccuracy())) {
			// FIT gps_accuracy is horizontal accuracy in metres; GPX <hdop> is unitless HDOP. Approximate HDOP ≈ metres / UERE
			// using a typical consumer-GPS ~6 m effective UERE (error ≈ HDOP × UERE); not an exact chipset mapping.
			p.setHdop(mesg.getGpsAccuracy().floatValue() / 6.0f);
		}
		return p;
	}

	private static void copyStandardTrackPointSensors(WptPt p, RecordMesg m, Sport sessionSport) {
		Map<String, String> ext = p.getExtensionsToWrite();
		if (usableUint8Short(m.getHeartRate())) {
			ext.put(PointAttributes.SENSOR_TAG_HEART_RATE, m.getHeartRate().toString());
		}
		boolean skipCad = sessionSport == Sport.WALKING || sessionSport == Sport.HIKING;
		if (!skipCad) {
			Float cad256 = m.getCadence256();
			if (usableFloat(cad256)) {
				ext.put(PointAttributes.SENSOR_TAG_CADENCE, formatCadence(cad256 / 256.0f));
			} else if (usableUint8Short(m.getCadence())) {
				ext.put(PointAttributes.SENSOR_TAG_CADENCE, m.getCadence().toString());
			}
		}
		if (usableUInt16(m.getPower())) {
			ext.put(PointAttributes.SENSOR_TAG_BIKE_POWER, m.getPower().toString());
		}
		if (usableSint8(m.getTemperature())) {
			ext.put(PointAttributes.SENSOR_TAG_TEMPERATURE_A, m.getTemperature().toString());
		}
	}

	private static String formatCadence(float cadence) {
		return String.format(Locale.ROOT, "%.2f", (double) cadence);
	}

	private static String gpxAuthorDeviceName(@Nullable Integer garminProductId) {
		if (!usableUInt16(garminProductId)) {
			return GPX_AUTHOR_BRAND_GARMIN;
		}
		String code = GarminProduct.getStringFromValue(garminProductId);
		if (code == null || code.isBlank()) {
			return GPX_AUTHOR_BRAND_GARMIN;
		}
		String label = humanizeGarminProductCode(code);
		return GPX_AUTHOR_BRAND_GARMIN + " " + label;
	}

	private static String humanizeGarminProductCode(String code) {
		String s = code.trim().replace('_', ' ');
		String[] tokens = s.split("\\s+");
		StringBuilder sb = new StringBuilder();
		for (String token : tokens) {
			if (token.isEmpty()) {
				continue;
			}
			String part = splitLettersFromTrailingDigits(token);
			if (!sb.isEmpty()) {
				sb.append(' ');
			}
			sb.append(titleCaseGarminToken(part));
		}
		return sb.toString();
	}

	private static String titleCaseGarminToken(String token) {
		if (token.isEmpty()) {
			return token;
		}
		if (token.chars().allMatch(Character::isDigit)) {
			return token;
		}
		if (token.length() == 1) {
			return token.toUpperCase(Locale.ROOT);
		}
		return Character.toUpperCase(token.charAt(0)) + token.substring(1).toLowerCase(Locale.ROOT);
	}

	private static @Nullable Integer garminProductFromFileId(FileIdMesg m) {
		Integer man = m.getManufacturer();
		if (!usableUInt16(man) || !isGarminManufacturer(man)) {
			return null;
		}
		Integer gp = resolveGarminProductField(m.getGarminProduct(), m.getProduct());
		return usableUInt16(gp) ? gp : null;
	}

	private static boolean isGarminManufacturer(@Nullable Integer manufacturer) {
		return manufacturer != null && manufacturer == Manufacturer.GARMIN;
	}

	private static @Nullable Integer resolveGarminProductField(@Nullable Integer garminProduct, @Nullable Integer product) {
		if (usableUInt16(garminProduct)) {
			return garminProduct;
		}
		if (usableUInt16(product)) {
			return product;
		}
		return null;
	}

	private static String splitLettersFromTrailingDigits(String token) {
		int i = 0;
		while (i < token.length() && Character.isLetter(token.charAt(i))) {
			i++;
		}
		if (i > 0 && i < token.length() && Character.isDigit(token.charAt(i))) {
			return token.substring(0, i) + " " + token.substring(i);
		}
		return token;
	}

	private static RouteActivity routeActivityFromFitSport(Sport sport) {
		if (sport == null || sport == Sport.INVALID) {
			return null;
		}
		String tag = sport.name().toLowerCase(Locale.ROOT);
		RouteActivityHelper h = RouteActivityHelper.INSTANCE;
		RouteActivity byTag = h.findActivityByTag(tag);
		if (byTag != null) {
			return byTag;
		}
		return h.findRouteActivity(tag);
	}

}
