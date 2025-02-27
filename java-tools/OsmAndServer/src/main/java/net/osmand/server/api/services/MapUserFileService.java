package net.osmand.server.api.services;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import net.osmand.data.QuadRect;
import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.api.repo.ShareFileRepository;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.Metadata;
import net.osmand.shared.gpx.primitives.WptPt;
import okio.Buffer;
import okio.Source;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static net.osmand.server.api.services.UserdataService.FILE_TYPE_FAVOURITES;
import static net.osmand.server.api.services.UserdataService.FILE_TYPE_GPX;

@Service
public class MapUserFileService {

	protected static final Log LOG = LogFactory.getLog(MapUserFileService.class);

	@Autowired
	WebGpxParser webGpxParser;

	@Autowired
	PremiumUserDevicesRepository userDevicesRepository;

	@Autowired
	PremiumUserFilesRepository userFilesRepository;

	@Autowired
	protected GpxService gpxService;

	@Autowired
	ShareFileService shareFileService;

	@Autowired
	TrackAnalyzerService trackAnalyzerService;

	@Autowired
	UserdataService userdataService;

	private static final String BBOX = "bbox";
	private static final String METADATA = "metadata";
	private static final String FAV_POINT_GROUPS = "pointGroups";
	public static final String ANALYSIS = "analysis";
	public static final String SHARE = "share";

	public static final String SRTM_ANALYSIS = "srtm-analysis";
	private static final String DONE_SUFFIX = "-done";

	public static final String UPDATETIME = "updatetime";
	public static final String UPDATE_DETAILS = "update";

	private static final String ERROR_DETAILS = "error";
	private static final long ERROR_LIFETIME = 7 * 86400000L; // 1 week

	private static final long ANALYSIS_RERUN = 1738674947073L; // 04-02-2025

	Gson gson = new Gson();

	Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();

	public record UserFileUpdate(String name, String type, boolean isError, String time) {
	}


	public ResponseEntity<String> refreshListFiles(List<UserFileUpdate> files, PremiumUserDevicesRepository.PremiumUserDevice dev) throws IOException {
		List<ShareFileRepository.ShareFile> shareList = shareFileService.getFilesByOwner(dev.userid);
		List<PremiumUserFilesRepository.UserFileNoData> result = new ArrayList<>();
		for (UserFileUpdate file : files) {
			if (file.isError) {
				if (file.time == null) {
					continue;
				}
				long time = Long.parseLong(file.time);
				if (System.currentTimeMillis() - time > ERROR_LIFETIME) {
					continue;
				}
			}

			UserdataController.UserFilesResults res = userdataService.generateFiles(dev.userid, file.name, false, true, file.type);
			if (res.uniqueFiles.isEmpty()) {
				LOG.error(String.format("refreshListFiles error: no files found for %s", file.name));
				continue;
			}
			if (res.uniqueFiles.size() > 1) {
				LOG.error(String.format("refreshListFiles error: expected a single file, but got %d files", res.uniqueFiles.size()));
				continue;
			}
			PremiumUserFilesRepository.UserFileNoData nd = res.uniqueFiles.get(0);
			Optional<PremiumUserFilesRepository.UserFile> of = userFilesRepository.findById(nd.id);
			boolean isTrack = file.type.equals(FILE_TYPE_GPX);
			if (of.isPresent()) {
				GpxTrackAnalysis analysis = null;
				GpxFile gpxFile = null;
				QuadRect bbox = null;
				PremiumUserFilesRepository.UserFile uf = of.get();
				JsonObject details = uf.details;
				InputStream in;
				try {
					in = uf.data != null ? new ByteArrayInputStream(uf.data) : userdataService.getInputStream(uf);
				} catch (Exception e) {
					String isError = String.format(
							"web-list-files-error: input-stream-error %s id=%d userid=%d error (%s)",
							uf.name, uf.id, uf.userid, e.getMessage());
					LOG.error(isError);
					saveError(details, uf, isError);
					continue;
				}
				if (in != null) {
					in = new GZIPInputStream(in);
					try (Source source = new Buffer().readFrom(in)) {
						gpxFile = GpxUtilities.INSTANCE.loadGpxFile(source);
					} catch (IOException e) {
						String loadError = String.format(
								"web-list-files-error: load-gpx-error %s id=%d userid=%d error (%s)",
								uf.name, uf.id, uf.userid, e.getMessage());
						LOG.error(loadError);
						saveError(details, uf, loadError);
						continue;
					}
					if (gpxFile.getError() != null) {
						String corruptedError = String.format(
								"web-list-files-error: corrupted-gpx-file %s id=%d userid=%d error (%s)",
								uf.name, uf.id, uf.userid, gpxFile.getError().getMessage());
						LOG.error(corruptedError);
						saveError(details, uf, corruptedError);
						continue;
					}
					if (isTrack) {
						analysis = getAnalysis(uf, gpxFile);
						List<WptPt> allPoints = gpxFile.getAllSegmentsPoints();
						bbox = trackAnalyzerService.calculateQuadRect(allPoints);
					}
				} else {
					String noIsError = String.format(
							"web-list-files-error: no-input-stream %s id=%d userid=%d", uf.name, uf.id, uf.userid);
					LOG.error(noIsError);
					saveError(details, uf, noIsError);
				}
				boolean isFavorite = file.type.equals(FILE_TYPE_FAVOURITES);
				boolean isSharedFile = isShared(nd, shareList);
				JsonObject newDetails = preparedDetails(gpxFile, analysis, bbox, isTrack, isFavorite, isSharedFile);
				saveDetails(newDetails, ANALYSIS, uf);
				nd.details = uf.details.deepCopy();
				result.add(nd);
			}
		}
		return ResponseEntity.ok(gson.toJson(result));
	}

	public JsonObject preparedDetails(GpxFile gpxFile, GpxTrackAnalysis analysis, QuadRect bbox, boolean isTrack, boolean isFavorite, boolean isShared) {
		JsonObject details = new JsonObject();
		if (gpxFile != null) {
			addMetadata(details, gpxFile);
			if (isFavorite) {
				addFavData(details, gpxFile);
			}
			if (isTrack) {
				addTrackData(details, analysis, bbox);
			}
		}
		details.add(SHARE, gson.toJsonTree(isShared));
		details.addProperty(UPDATETIME, System.currentTimeMillis());

		return details;
	}

	public boolean detailsPresent(JsonObject details) {
		return details != null
				&& !details.has(ERROR_DETAILS)
				&& details.has(UPDATETIME)
				&& details.get(UPDATETIME).getAsLong() >= ANALYSIS_RERUN;
	}

	public boolean analysisPresent(String tag, JsonObject details) {
		return details != null && details.has(tag + DONE_SUFFIX)
				&& details.get(tag + DONE_SUFFIX).getAsLong() >= ANALYSIS_RERUN
				&& details.has(tag) && !details.get(tag).isJsonNull();
	}

	public boolean analysisPresentFavorites(String tag, JsonObject details) {
		return details != null
				&& details.has(tag + DONE_SUFFIX)
				&& details.has(FAV_POINT_GROUPS)
				&& !details.get(FAV_POINT_GROUPS).getAsString().equals("{}")
				&& details.get(tag + DONE_SUFFIX).getAsLong() >= ANALYSIS_RERUN;
	}

	public boolean isShared(PremiumUserFilesRepository.UserFileNoData file, List<ShareFileRepository.ShareFile> shareList) {
		for (ShareFileRepository.ShareFile sf : shareList) {
			if (sf.getFilepath().equals(file.name) && sf.getType().equals(file.type)) {
				return true;
			}
		}
		return false;
	}

	public void addDeviceInformation(PremiumUserFilesRepository.UserFileNoData file, Map<Integer, String> devices) {
		String deviceInfo = devices.get(file.deviceid);
		if (deviceInfo == null) {
			PremiumUserDevicesRepository.PremiumUserDevice device = userDevicesRepository.findById(file.deviceid);
			if (device != null && device.brand != null && device.model != null) {
				deviceInfo = device.brand + "__model__" + device.model;
				devices.put(file.deviceid, deviceInfo);
			}
		}
		file.setDeviceInfo(deviceInfo);
	}

	public boolean analysisPresent(String tag, PremiumUserFilesRepository.UserFile userFile) {
		if (userFile == null) {
			return false;
		}
		JsonObject details = userFile.details;
		return analysisPresent(tag, details);
	}

	public void saveAnalysis(String tag, PremiumUserFilesRepository.UserFile file, GpxTrackAnalysis analysis) {
		if (analysis != null) {
			if (file.details == null) {
				file.details = new JsonObject();
			}
			Map<String, Object> res = getDetails(analysis);
			if (!res.isEmpty()) {
				file.details.add(tag, gsonWithNans.toJsonTree(res));
			}
		}
		saveDetails(file.details, tag, file);
	}

	public void saveDetails(JsonObject newDetails, String tag, PremiumUserFilesRepository.UserFile file) {
		newDetails.addProperty(tag + DONE_SUFFIX, System.currentTimeMillis());
		file.details = newDetails;
		userFilesRepository.save(file);
	}

	public GpxTrackAnalysis getAnalysis(PremiumUserFilesRepository.UserFile file, GpxFile gpxFile) {
		gpxFile.setPath(file.name);
		GpxTrackAnalysis analysis = gpxFile.getAnalysis(0); // keep 0
		gpxService.cleanupFromNan(analysis);

		return analysis;
	}

	private void addMetadata(JsonObject details, GpxFile gpxFile) {
		Metadata metadata = gpxFile.getMetadata();
		if (!metadata.isEmpty()) {
			metadata.setDesc(null);
		}
		details.add(METADATA, gson.toJsonTree(metadata));
	}

	private void addFavData(JsonObject details, GpxFile gpxFile) {
		Map<String, WebGpxParser.WebPointsGroup> groups = webGpxParser.getPointsGroups(gpxFile);
		if (!groups.isEmpty()) {
			Map<String, Map<String, String>> pointGroupsAnalysis = new HashMap<>();
			groups.keySet().forEach(k -> {
				Map<String, String> groupInfo = new HashMap<>();
				WebGpxParser.WebPointsGroup group = groups.get(k);
				groupInfo.put("color", group.color);
				groupInfo.put("groupSize", String.valueOf(group.points.size()));
				groupInfo.put("hidden", String.valueOf(isHidden(group)));
				pointGroupsAnalysis.put(k, groupInfo);
			});
			details.add(FAV_POINT_GROUPS, gson.toJsonTree(gsonWithNans.toJson(pointGroupsAnalysis)));
		}
	}

	private void addTrackData(JsonObject details, GpxTrackAnalysis analysis, QuadRect bbox) {
		if (analysis != null) {
			Map<String, Object> res = getDetails(analysis);
			if (!res.isEmpty()) {
				details.add(ANALYSIS, gsonWithNans.toJsonTree(res));
			}
		}
		if (bbox != null) {
			JsonObject bboxJson = new JsonObject();
			bboxJson.addProperty("left", bbox.left);
			bboxJson.addProperty("top", bbox.top);
			bboxJson.addProperty("right", bbox.right);
			bboxJson.addProperty("bottom", bbox.bottom);
			details.add(BBOX, bboxJson);
		}
	}

	private void saveError(JsonObject details, PremiumUserFilesRepository.UserFile uf, String error) {
		if (details == null) {
			details = new JsonObject();
		}
		details.addProperty(ERROR_DETAILS, error);
		details.addProperty(UPDATETIME, System.currentTimeMillis());
		uf.details = details;

		userFilesRepository.save(uf);
	}

	private boolean isHidden(WebGpxParser.WebPointsGroup group) {
		for (WebGpxParser.Wpt wpt : group.points) {
			if (wpt.hidden != null && wpt.hidden.equals("true")) {
				return true;
			}
		}
		return false;
	}

	private Map<String, Object> getDetails(GpxTrackAnalysis analysis) {
		if (analysis != null) {
			Map<String, Object> res = new HashMap<>();
			analysis.getPointAttributes().clear();
			res.put("totalDistance", analysis.getTotalDistance());
			res.put("startTime", analysis.getStartTime());
			res.put("endTime", analysis.getEndTime());
			res.put("timeMoving", analysis.getTimeMoving());
			res.put("points", analysis.getPoints());
			res.put("wptPoints", analysis.getWptPoints());

			return res;
		}
		return Collections.emptyMap();
	}
}
