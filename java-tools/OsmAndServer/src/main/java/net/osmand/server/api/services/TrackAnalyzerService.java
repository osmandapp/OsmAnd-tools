package net.osmand.server.api.services;

import net.osmand.data.LatLon;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import okio.Buffer;
import okio.Source;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static net.osmand.router.RouteExporter.OSMAND_ROUTER_V2;

@Service
public class TrackAnalyzerService {

	@Autowired
	UserdataService userdataService;

	@Autowired
	protected CloudUserFilesRepository filesRepository;

	private static final int ZOOM = 12;
	private static final int SHORT_LINK_ZOOM = ZOOM - 8;
	private static final double MIN_DISTANCE_FOR_SHORTLINK = 400.0;

	protected static final Log LOG = LogFactory.getLog(TrackAnalyzerService.class);
	static final double MIN_DIST_THRESHOLD = 5; // meters
	static final double MAX_DIST_THRESHOLD = 30; // meters
	static final Map<String, Number> DEFAULT_VALUES = Map.of(
			"speed", -1L,
			"elevation", 99999.0
	);

	public static class TrackAnalyzerRequest {
		final List<Map<String, Double>> points;
		final List<String> folders;

		TrackAnalyzerRequest(List<Map<String, Double>> points, List<String> folders) {
			this.points = points;
			this.folders = folders;
		}
	}

	public static class TrackAnalyzerResponse {
		final Map<String, List<TrkSegment>> segments;
		final Map<String, List<Map<String, String>>> trackAnalysis;
		final Set<CloudUserFilesRepository.UserFileNoData> files;

		public TrackAnalyzerResponse() {
			this.segments = new HashMap<>();
			this.trackAnalysis = new HashMap<>();
			this.files = new HashSet<>();
		}
	}

	public TrackAnalyzerResponse getTracksBySegment(TrackAnalyzerRequest analyzerRequest, CloudUserDevicesRepository.CloudUserDevice dev) throws IOException {
		TrackAnalyzerResponse analysisResponse = new TrackAnalyzerResponse();

		List<WptPt> wptPoints = analyzerRequest.points.stream()
				.map(point -> new WptPt(point.get("lat"), point.get("lon")))
				.toList();

		//when we have one point, check if the track intersects with the point and response all track stats
		boolean useOnePoint = wptPoints.size() == 1;
		String[] tiles = getQuadTileShortlinks(wptPoints);

		//get files for analysis
		UserdataController.UserFilesResults userFiles = userdataService.generateGpxFilesByQuadTiles(dev.userid, false,  tiles);
		List<CloudUserFilesRepository.UserFileNoData> filesForAnalysis = getFilesForAnalysis(userFiles, analyzerRequest.folders);

		if (Algorithms.isEmpty(analyzerRequest.points)) {
			return null;
		}
		for (CloudUserFilesRepository.UserFileNoData file : filesForAnalysis) {
			processFileForSegments(file, useOnePoint, wptPoints, MIN_DIST_THRESHOLD, analysisResponse);
		}
		if (analysisResponse.segments.isEmpty()) {
			for (CloudUserFilesRepository.UserFileNoData file : filesForAnalysis) {
				processFileForSegments(file, useOnePoint, wptPoints, MAX_DIST_THRESHOLD, analysisResponse);
			}
		}
		return analysisResponse;
	}

	private void processFileForSegments(
			CloudUserFilesRepository.UserFileNoData file,
			boolean useOnePoint,
			List<WptPt> wptPoints,
			double distThreshold,
			TrackAnalyzerResponse analysisResponse) throws IOException {

		Optional<CloudUserFilesRepository.UserFile> of = filesRepository.findById(file.id);
		if (of.isEmpty()) {
			return;
		}

		CloudUserFilesRepository.UserFile uf = of.get();
		GpxFile gpxFile = getGpxFile(uf);
		if (gpxFile == null) {
			return;
		}

		for (Track t : gpxFile.getTracks()) {
			for (TrkSegment s : t.getSegments()) {
				List<TrkSegment> segments = useOnePoint
						? processSegmentsForOnePoint(uf.name, s, wptPoints.get(0), distThreshold)
						: processSegments(uf.name, s, wptPoints.get(0), wptPoints.get(1), distThreshold);

				if (!segments.isEmpty()) {
					analysisResponse.segments.put(uf.name, segments);
					analysisResponse.files.add(file);

					List<Map<String, String>> statResults = new ArrayList<>();
					for (TrkSegment seg : segments) {
						GpxFile g = new GpxFile(OSMAND_ROUTER_V2);
						g.getTracks().add(new Track());
						g.getTracks().get(0).getSegments().add(seg);
						GpxTrackAnalysis analysis = g.getAnalysis(0);

						Map<String, String> trackAnalysisData = getSegmentAnalysis(analysis);
						trackAnalysisData.put("date", String.valueOf(GpxUtilities.INSTANCE.getCreationTime(gpxFile)));
						statResults.add(trackAnalysisData);
					}
					analysisResponse.trackAnalysis.put(uf.name, statResults);
				}
			}
		}
	}

	@NotNull
	private static Map<String, String> getSegmentAnalysis(GpxTrackAnalysis analysis) {
		Map<String, String> trackAnalysisData = new HashMap<>();
		final String DEFAULT = "NaN";
		// add speed
		float avgSpeed = analysis.getAvgSpeed();
		if (avgSpeed == DEFAULT_VALUES.get("speed").floatValue()) {
			// track without speed
			trackAnalysisData.put("minSpeed", DEFAULT);
			trackAnalysisData.put("avgSpeed", DEFAULT);
			trackAnalysisData.put("maxSpeed", DEFAULT);
		} else {
			trackAnalysisData.put("minSpeed", String.valueOf(analysis.getMinSpeed()));
			trackAnalysisData.put("avgSpeed", String.valueOf(avgSpeed));
			trackAnalysisData.put("maxSpeed", String.valueOf(analysis.getMaxSpeed()));
		}
		// add elevation
		double minElevation = analysis.getMinElevation();
		if (minElevation == DEFAULT_VALUES.get("elevation").doubleValue()) {
			// track without elevation
			trackAnalysisData.put("minElevation", DEFAULT);
			trackAnalysisData.put("maxElevation", DEFAULT);
			trackAnalysisData.put("avgElevation", DEFAULT);
			trackAnalysisData.put("diffElevationUp", DEFAULT);
			trackAnalysisData.put("diffElevationDown", DEFAULT);
		} else {
			trackAnalysisData.put("minElevation", String.valueOf(minElevation));
			trackAnalysisData.put("maxElevation", String.valueOf(analysis.getMaxElevation()));
			trackAnalysisData.put("avgElevation", String.valueOf(analysis.getAvgElevation()));
			trackAnalysisData.put("diffElevationUp", String.valueOf(analysis.getDiffElevationUp()));
			trackAnalysisData.put("diffElevationDown", String.valueOf(analysis.getDiffElevationDown()));
		}
		// add other data
		double duration = analysis.getDurationInMs();
		trackAnalysisData.put("duration", duration == 0.0 ? DEFAULT : String.valueOf(duration));

		double timeMoving = analysis.getTimeMoving();
		trackAnalysisData.put("timeMoving", timeMoving == 0.0 ? DEFAULT : String.valueOf(timeMoving));
		double timeSpan = analysis.getTimeSpan();
		trackAnalysisData.put("timeSpan", timeSpan == 0.0 ? DEFAULT : String.valueOf(timeSpan));
		long startTime = analysis.getStartTime();
		trackAnalysisData.put("startTime", (startTime == 0L) ? DEFAULT : String.valueOf(startTime));
		long endTime = analysis.getEndTime();
		trackAnalysisData.put("endTime", (endTime == 0L) ? DEFAULT : String.valueOf(endTime));

		double totalDist = analysis.getTotalDistance();
		trackAnalysisData.put("totalDist", totalDist == 0.0 ? DEFAULT : String.valueOf(totalDist));

		return trackAnalysisData;
	}

	private List<CloudUserFilesRepository.UserFileNoData> getFilesForAnalysis(UserdataController.UserFilesResults userFiles, List<String> folders) {
		List<CloudUserFilesRepository.UserFileNoData> filesForAnalysis = new ArrayList<>();

		if (Algorithms.isEmpty(folders)) {
			filesForAnalysis.addAll(userFiles.uniqueFiles);
			return filesForAnalysis;
		}

		for (CloudUserFilesRepository.UserFileNoData file : userFiles.uniqueFiles) {

			boolean shouldAdd = folders.stream().anyMatch(folderName ->
					file.name.startsWith(folderName + "/") || (folderName.equals("_default_") && file.name.indexOf('/') == -1)
			);

			if (shouldAdd) {
				filesForAnalysis.add(file);
			}
		}
		return filesForAnalysis;
	}

	private GpxFile getGpxFile(CloudUserFilesRepository.UserFile uf) throws IOException {
		InputStream in;
		try {
			in = uf.data != null ? new ByteArrayInputStream(uf.data) : userdataService.getInputStream(uf);
		} catch (Exception e) {
			LOG.error(String.format(
					"getGpxFile-error: input-stream-error %s id=%d userid=%d error (%s)",
					uf.name, uf.id, uf.userid, e.getMessage()));
			return null;
		}
		if (in != null) {
			in = new GZIPInputStream(in);
			GpxFile gpxFile;
			try (Source source = new Buffer().readFrom(in)) {
				gpxFile = GpxUtilities.INSTANCE.loadGpxFile(source);
			} catch (IOException e) {
				LOG.error(String.format(
						"getGpxFile-error: load-gpx-error %s id=%d userid=%d error (%s)",
						uf.name, uf.id, uf.userid, e.getMessage()));
				return null;
			}
			if (gpxFile.getError() != null) {
				LOG.error(String.format(
						"getGpxFile-error: corrupted-gpx-file %s id=%d userid=%d error (%s)",
						uf.name, uf.id, uf.userid, gpxFile.getError().getMessage()));
				return null;
			}
			return gpxFile;
		}
		return null;
	}

	private List<TrkSegment> processSegments(String trackName, TrkSegment s, WptPt start, WptPt end, double distThreshold) {
		List<TrkSegment> res = new ArrayList<>();
		List<WptPt> points = s.getPoints();
		int startInd = -1;
		for (int i = 1; i < points.size(); ) {
			WptPt pnt = startInd == -1 ? start : end;
			double dist = MapUtils.getOrthogonalDistance(pnt.getLatitude(), pnt.getLongitude(),
					points.get(i - 1).getLat(), points.get(i - 1).getLon(),
					points.get(i).getLat(), points.get(i).getLon());

			if (dist < distThreshold) {
				int ind = i;
				for (int j = ind + 1; j < points.size() && j < ind + 10; j++) {
					double d2 = MapUtils.getOrthogonalDistance(pnt.getLatitude(), pnt.getLongitude(),
							points.get(j - 1).getLat(), points.get(j - 1).getLon(),
							points.get(j).getLat(), points.get(j).getLon());

					if (d2 < dist) {
						dist = d2;
						i = j;
					}
				}
				if (startInd == -1) {
					startInd = i;
				} else {
					int finalInd = i;
					if (startInd >= points.size() - 1 || finalInd >= points.size()) {
						return res;
					}

					TrkSegment r = new TrkSegment();

					LatLon startProj = MapUtils.getProjection(start.getLatitude(), start.getLongitude(),
							lat(s, startInd - 1), lon(s, startInd - 1), lat(s, startInd), lon(s, startInd));

					double startDist = MapUtils.getDistance(lat(s, startInd - 1), lon(s, startInd - 1),
							lat(s, startInd), lon(s, startInd));
					double stPercent = startDist == 0 ? 0 :
							MapUtils.getDistance(startProj, lat(s, startInd), lon(s, startInd)) / startDist;

					WptPt st = new WptPt(startProj.getLatitude(), startProj.getLongitude());
					st.setTime(time(s, startInd - 1) + (long) (stPercent * (time(s, startInd) - time(s, startInd - 1))));
					st.setEle(points.get(startInd).getEle());

					LatLon endProj = MapUtils.getProjection(end.getLatitude(), end.getLongitude(),
							lat(s, finalInd - 1), lon(s, finalInd - 1), lat(s, finalInd), lon(s, finalInd));

					double endDist = MapUtils.getDistance(lat(s, finalInd - 1), lon(s, finalInd - 1),
							lat(s, finalInd), lon(s, finalInd));
					double enPercent = endDist == 0 ? 0 :
							MapUtils.getDistance(endProj, lat(s, finalInd), lon(s, finalInd)) / endDist;

					WptPt en = new WptPt(endProj.getLatitude(), endProj.getLongitude());
					en.setTime(time(s, finalInd - 1) + (long) (enPercent * (time(s, finalInd) - time(s, finalInd - 1))));
					en.setEle(points.get(finalInd).getEle());

					r.getPoints().add(st);
					for (int k = startInd + 1; k <= finalInd; k++) {
						r.getPoints().add(points.get(k));
					}
					r.getPoints().add(en);
					r.setName(trackName);
					res.add(r);
					startInd = -1;
				}
			}
			i++;
		}
		return res;
	}

	private List<TrkSegment> processSegmentsForOnePoint(String trackName, TrkSegment s, WptPt point, double distThreshold) {
		List<TrkSegment> res = new ArrayList<>();

		int startInd = -1;
		for (int i = 0; i < s.getPoints().size(); i++) {
			double dist = MapUtils.getDistance(
					point.getLatitude(), point.getLongitude(),
					s.getPoints().get(i).getLat(), s.getPoints().get(i).getLon()
			);
			if (dist < distThreshold) {
				startInd = i;
				break;
			}
		}

		if (startInd != -1) {
			TrkSegment segment = new TrkSegment();
			for (int i = startInd; i < s.getPoints().size(); i++) {
				segment.getPoints().add(s.getPoints().get(i));
			}
			segment.setName(trackName);
			res.add(segment);
		}

		return res;
	}

	private double lat(TrkSegment s, int ind) {
		return s.getPoints().get(ind).getLat();
	}

	private double lon(TrkSegment s, int ind) {
		return s.getPoints().get(ind).getLon();
	}

	private long time(TrkSegment s, int ind) {
		return s.getPoints().get(ind).getTime();
	}

	public String[] getQuadTileShortlinks(List<WptPt> points) {
		if (points == null || points.isEmpty()) {
			return new String[0];
		}
		Set<String> shortLinkTiles = new TreeSet<>();
		WptPt firstPoint = points.get(0);
		shortLinkTiles.add(MapUtils.createShortLinkString(firstPoint.getLat(), firstPoint.getLon(), SHORT_LINK_ZOOM));

		WptPt lastAddedPoint = firstPoint;

		for (int i = 1; i < points.size(); i++) {
			WptPt point = points.get(i);

			double distance = MapUtils.getDistance(lastAddedPoint.getLat(), lastAddedPoint.getLon(), point.getLat(), point.getLon());

			if (distance > MIN_DISTANCE_FOR_SHORTLINK) {
				shortLinkTiles.add(MapUtils.createShortLinkString(point.getLat(), point.getLon(), SHORT_LINK_ZOOM));
				lastAddedPoint = point;
			}
		}

		WptPt lastPoint = points.get(points.size() - 1);
		shortLinkTiles.add(MapUtils.createShortLinkString(lastPoint.getLat(), lastPoint.getLon(), SHORT_LINK_ZOOM));

		return shortLinkTiles.toArray(new String[0]);
	}
}
