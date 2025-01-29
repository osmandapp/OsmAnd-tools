package net.osmand.server.api.services;

import lombok.Getter;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
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

import static net.osmand.data.QuadRect.intersects;

@Service
public class TrackAnalyzerService {

	@Autowired
	UserdataService userdataService;

	@Autowired
	protected PremiumUserFilesRepository filesRepository;

	protected static final Log LOG = LogFactory.getLog(TrackAnalyzerService.class);
	static final double DIST_THRESHOLD = 50; // meters

	@Getter
	public static class TrackAnalyzerRequest {
		List<Map<String, Double>> points;
		List<String> folders;

		TrackAnalyzerRequest(List<Map<String, Double>> points, List<String> folders) {
			this.points = points;
			this.folders = folders;
		}
	}

	public static class TrackAnalyzerResponse {
		Map<String, List<TrkSegment>> segments;
		Map<String, List<Map<String, Double>>> trackAnalysis;
		Set<PremiumUserFilesRepository.UserFileNoData> files;

		TrackAnalyzerResponse(Map<String, List<TrkSegment>> segments, Map<String, List<Map<String, Double>>> trackAnalysis, Set<PremiumUserFilesRepository.UserFileNoData> files) {
			this.segments = segments;
			this.trackAnalysis = trackAnalysis;
			this.files = files;
		}
	}

	public TrackAnalyzerResponse getTracksBySegment(TrackAnalyzerRequest analyzerRequest, PremiumUserDevicesRepository.PremiumUserDevice dev) throws IOException {
		TrackAnalyzerResponse analysisResponse = new TrackAnalyzerResponse(new HashMap<>(), new HashMap<>(), new HashSet<>());

		//get files for analysis
		UserdataController.UserFilesResults userFiles = userdataService.generateFiles(dev.userid, null, false, true, "GPX");
		List<PremiumUserFilesRepository.UserFileNoData> filesForAnalysis = getFilesForAnalysis(userFiles, analyzerRequest.folders);

		//get points
		List<LatLon> latLonPoints = analyzerRequest.getPoints().stream()
				.map(point -> new LatLon(point.get("lat"), point.get("lon")))
				.toList();
		if (analyzerRequest.points == null) {
			return null;
		}
		QuadRect bboxPoints = latLonPoints.size() == 2 ? getBboxByPoints(latLonPoints) : null;
		//when we have one point, check if the track intersects with the point and response all track stats
		boolean useOnePoint = bboxPoints == null;

		//process files
		for (PremiumUserFilesRepository.UserFileNoData file : filesForAnalysis) {
			Optional<PremiumUserFilesRepository.UserFile> of = filesRepository.findById(file.id);
			if (of.isPresent()) {
				PremiumUserFilesRepository.UserFile uf = of.get();
				QuadRect trackBbox = null;
				if (uf.details != null) {
					trackBbox = uf.getBbox();
					if (trackBbox != null && isOutOfBounds(trackBbox, latLonPoints, bboxPoints, useOnePoint)) {
						continue;
					}
				}
				GpxFile gpxFile = getGpxFile(uf);
				if (gpxFile == null) {
					continue;
				}
				if (trackBbox == null) {
					List<WptPt> allPoints = gpxFile.getAllSegmentsPoints();
					trackBbox = calculateQuadRect(allPoints);
					if (isOutOfBounds(trackBbox, latLonPoints, bboxPoints, useOnePoint)) {
						continue;
					}
				}
				for (Track t : gpxFile.getTracks()) {
					for (TrkSegment s : t.getSegments()) {
						List<TrkSegment> segments = useOnePoint
								? processSegmentsForOnePoint(uf.name, s, latLonPoints.get(0))
								: processSegments(uf.name, s, latLonPoints.get(0), latLonPoints.get(1));
						if (!segments.isEmpty()) {
							analysisResponse.segments.put(uf.name, segments);
							analysisResponse.files.add(file);
							GpxTrackAnalysis analysis;
							if (useOnePoint) {
								analysis = gpxFile.getAnalysis(0);
								Map<String, Double> trackAnalysisData = getSegmentAnalysis(analysis, uf);
								analysisResponse.trackAnalysis.put(uf.name, List.of(trackAnalysisData));
							} else {
								List<Map<String, Double>> statResults = new ArrayList<>();
								for (TrkSegment seg : segments) {
									GpxFile g = new GpxFile("");
									g.getTracks().add(new Track());
									g.getTracks().get(0).getSegments().add(seg);
									analysis = g.getAnalysis(0);

									Map<String, Double> trackAnalysisData = getSegmentAnalysis(analysis, uf);
									statResults.add(trackAnalysisData);
								}
								analysisResponse.trackAnalysis.put(uf.name, statResults);
							}
						}
					}
				}
			}
		}
		return analysisResponse;
	}

	private boolean isOutOfBounds(QuadRect trackBbox, List<LatLon> latLonPoints, QuadRect bboxPoints, boolean useOnePoint) {
		if (useOnePoint) {
			return !intersectsWithPoint(trackBbox, latLonPoints.get(0));
		} else {
			return !intersects(trackBbox, bboxPoints);
		}
	}

	@NotNull
	private static Map<String, Double> getSegmentAnalysis(GpxTrackAnalysis analysis, PremiumUserFilesRepository.UserFile uf) {
		Map<String, Double> trackAnalysisData = new HashMap<>();

		trackAnalysisData.put("minSpeed", (double) analysis.getMinSpeed());
		trackAnalysisData.put("avgSpeed", (double) analysis.getAvgSpeed());
		trackAnalysisData.put("maxSpeed", (double) analysis.getMaxSpeed());
		trackAnalysisData.put("minElevation", analysis.getMinElevation());
		trackAnalysisData.put("maxElevation", analysis.getMaxElevation());
		trackAnalysisData.put("avgElevation", analysis.getAvgElevation());
		trackAnalysisData.put("diffElevationUp", analysis.getDiffElevationUp());
		trackAnalysisData.put("diffElevationDown", analysis.getDiffElevationDown());
		trackAnalysisData.put("date", (double) uf.updatetime.getTime());
		trackAnalysisData.put("duration", (double) analysis.getDurationInMs());
		trackAnalysisData.put("timeMoving", (double) analysis.getTimeMoving());
		trackAnalysisData.put("totalDist", (double) analysis.getTotalDistance());

		return trackAnalysisData;
	}

	private boolean intersectsWithPoint(QuadRect bbox, LatLon point) {
		return bbox != null && point.getLatitude() >= bbox.bottom && point.getLatitude() <= bbox.top &&
				point.getLongitude() >= bbox.left && point.getLongitude() <= bbox.right;
	}

	private List<PremiumUserFilesRepository.UserFileNoData> getFilesForAnalysis(UserdataController.UserFilesResults userFiles, List<String> folders) {
		List<PremiumUserFilesRepository.UserFileNoData> filesForAnalysis = new ArrayList<>();

		if (folders == null) {
			filesForAnalysis.addAll(userFiles.uniqueFiles);
			return filesForAnalysis;
		}

		for (PremiumUserFilesRepository.UserFileNoData file : userFiles.uniqueFiles) {

			boolean shouldAdd = folders.stream().anyMatch(folderName ->
					(folderName.equals("_default_") && file.name.indexOf('/') == -1) ||
							file.name.startsWith(folderName + "/")
			);

			if (shouldAdd) {
				filesForAnalysis.add(file);
			}
		}
		return filesForAnalysis;
	}

	private GpxFile getGpxFile(PremiumUserFilesRepository.UserFile uf) throws IOException {
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

	private QuadRect getBboxByPoints(List<LatLon> points) {
		List<WptPt> wptPoints = points.stream()
				.map(latLon -> new WptPt(latLon.getLatitude(), latLon.getLongitude()))
				.toList();
		return calculateQuadRect(wptPoints);
	}

	private List<TrkSegment> processSegments(String trackName, TrkSegment s, LatLon start, LatLon end) {
		List<TrkSegment> res = new ArrayList<>();
		int startInd = -1;
		for (int i = 1; i < s.getPoints().size(); ) {
			LatLon pnt = startInd == -1 ? start : end;
			double dist = MapUtils.getOrthogonalDistance(pnt.getLatitude(), pnt.getLongitude(), s.getPoints().get(i - 1).getLat(),
					s.getPoints().get(i - 1).getLon(), s.getPoints().get(i).getLat(), s.getPoints().get(i).getLon());
			if (dist < DIST_THRESHOLD) {
				// check next 10 points
				int ind = i;
				for (int j = ind + 1; j < s.getPoints().size() && j < ind + 10; j++) {
					double d2 = MapUtils.getOrthogonalDistance(pnt.getLatitude(), pnt.getLongitude(),
							s.getPoints().get(j - 1).getLat(), s.getPoints().get(j - 1).getLon(), s.getPoints().get(j).getLat(), s.getPoints().get(j).getLon());
					if (d2 < dist) {
						dist = d2;
						i = j;
					}
				}
				if (startInd == -1) {
					startInd = i;
				} else {
					int finalInd = i;
					// create new segment with start and end points
					TrkSegment r = new TrkSegment();
					// calculate start and end points
					LatLon startProj = MapUtils.getProjection(start.getLatitude(), start.getLongitude(),
							lat(s, startInd - 1), lon(s, startInd - 1), lat(s, startInd), lon(s, startInd));
					// calculate percentage of distance between startInd-1 and startInd
					double stPercent = MapUtils.getDistance(startProj, lat(s, startInd), lon(s, startInd)) /
							MapUtils.getDistance(lat(s, startInd - 1), lon(s, startInd - 1), lat(s, startInd), lon(s, startInd));
					// create new point with calculated lat, lon, time and ele
					WptPt st = new WptPt(startProj.getLatitude(), startProj.getLongitude());
					st.setTime(time(s, startInd - 1) + (long) (stPercent * (time(s, startInd) - time(s, startInd - 1))));
					st.setEle(s.getPoints().get(startInd).getEle());

					LatLon endProj = MapUtils.getProjection(end.getLatitude(), end.getLongitude(),
							lat(s, finalInd - 1), lon(s, finalInd - 1), lat(s, finalInd), lon(s, finalInd));
					double enPercent = MapUtils.getDistance(endProj, lat(s, finalInd), lon(s, finalInd)) /
							MapUtils.getDistance(lat(s, finalInd - 1), lon(s, finalInd - 1), lat(s, finalInd), lon(s, finalInd));
					WptPt en = new WptPt(endProj.getLatitude(), endProj.getLongitude());
					en.setTime(time(s, finalInd - 1) + (long) (enPercent * (time(s, finalInd) - time(s, finalInd - 1))));
					en.setEle(s.getPoints().get(finalInd).getEle());

					r.getPoints().add(st);
					// add all points between startInd and finalInd
					for (int k = startInd + 1; k <= finalInd; k++) {
						r.getPoints().add(s.getPoints().get(k));
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

	private List<TrkSegment> processSegmentsForOnePoint(String trackName, TrkSegment s, LatLon point) {
		List<TrkSegment> res = new ArrayList<>();

		for (int i = 0; i < s.getPoints().size(); i++) {
			double dist = MapUtils.getDistance(
					point.getLatitude(), point.getLongitude(),
					s.getPoints().get(i).getLat(), s.getPoints().get(i).getLon()
			);
			if (dist < DIST_THRESHOLD) {
				// check if we have previous and next points
				WptPt prevPoint = (i > 0) ? s.getPoints().get(i - 1) : null;
				WptPt nextPoint = (i < s.getPoints().size() - 1) ? s.getPoints().get(i + 1) : null;

				if (prevPoint != null && nextPoint != null) {
					TrkSegment segment = new TrkSegment();
					segment.getPoints().add(prevPoint); // add previous point
					segment.getPoints().add(s.getPoints().get(i)); // add current point
					segment.getPoints().add(nextPoint); // add next point
					segment.setName(trackName + " segment with point match");
					res.add(segment);
				}
				break; // we found the point, no need to continue
			}
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

	private QuadRect calculateQuadRect(List<WptPt> points) {
		if (points.isEmpty()) {
			return new QuadRect(0, 0, 0, 0);
		}

		double minLat = Double.MAX_VALUE;
		double maxLat = -Double.MAX_VALUE;
		double minLon = Double.MAX_VALUE;
		double maxLon = -Double.MAX_VALUE;

		for (WptPt point : points) {
			double lat = point.getLat();
			double lon = point.getLon();
			if (lat < minLat) minLat = lat;
			if (lat > maxLat) maxLat = lat;
			if (lon < minLon) minLon = lon;
			if (lon > maxLon) maxLon = lon;
		}

		return new QuadRect(minLon, maxLat, maxLon, minLat);
	}
}
