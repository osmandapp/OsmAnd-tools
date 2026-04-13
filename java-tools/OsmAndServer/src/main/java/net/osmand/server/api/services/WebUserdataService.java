package net.osmand.server.api.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import jakarta.servlet.http.HttpSession;
import jakarta.transaction.Transactional;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.controllers.pub.UserSessionResources;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.server.utils.exception.OsmAndPublicApiException;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.Metadata;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.shared.io.KFile;
import okio.Buffer;
import okio.Source;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static net.osmand.server.api.services.UserdataService.FILE_TYPE_GPX;
import static net.osmand.shared.IndexConstants.GPX_FILE_EXT;
import static net.osmand.shared.IndexConstants.GPX_FILE_PREFIX;

@Service
public class WebUserdataService {

	protected static final Log LOG = LogFactory.getLog(WebUserdataService.class);

	@Autowired
	WebGpxParser webGpxParser;

	@Autowired
	CloudUserDevicesRepository userDevicesRepository;

	@Autowired
	CloudUserFilesRepository userFilesRepository;

	@Autowired
	protected GpxService gpxService;

	@Autowired
	ShareFileService shareFileService;

	@Autowired
	TrackAnalyzerService trackAnalyzerService;

	@Autowired
	UserdataService userdataService;

	@Autowired
	UserSessionResources sessionResources;

	public static final String METADATA = "metadata";
	public static final String INFO_DATA_JSON = "data";
	private static final String FAV_POINT_GROUPS = "pointGroups";
	public static final String ANALYSIS = "analysis";
	public static final String ANALYSIS_ADDITIONAL = "analysisAdditional";
	public static final String SHARE = "share";
	private static final String AUTHOR = "author";
	private static final String HAS_ADVANCED_ROUTE = "hasAdvancedRoute";

	public static final String SRTM_ANALYSIS = "srtm-analysis";
	private static final String DONE_SUFFIX = "-done";

	public static final String UPDATETIME = "updatetime";
	public static final String UPDATE_DETAILS = "update";
	public static final String INFO_FILE_EXT = ".info";
	public static final String INFO_FILE_SUFFIX = GPX_FILE_EXT + INFO_FILE_EXT;

	private static final String ERROR_DETAILS = "error";
	private static final long ERROR_LIFETIME = 31 * 86400000L; // 1 month

	private static final long ANALYSIS_RERUN = 1775742653000L; // 09-04-2026

	Gson gson = new Gson();

	Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();

	public record UserFileUpdate(String name, String type, boolean isError, String time) {
	}

	public record TrackAnalysisJson(
			float totalDistance,
			long startTime,
			long endTime,
			long timeMoving,
			int points,
			int wptPoints
	) {
		public static TrackAnalysisJson from(GpxTrackAnalysis analysis) {
			return new TrackAnalysisJson(
					analysis.getTotalDistance(),
					analysis.getStartTime(),
					analysis.getEndTime(),
					analysis.getTimeMoving(),
					analysis.getPoints(),
					analysis.getWptPoints());
		}

		public void applyTo(GpxTrackAnalysis analysis) {
			if (analysis == null) {
				return;
			}
			analysis.setTotalDistance(totalDistance);
			analysis.setStartTime(startTime);
			analysis.setEndTime(endTime);
			analysis.setTimeMoving(timeMoving);
			analysis.setPoints(points);
			analysis.setWptPoints(wptPoints);
		}
	}

	public record TrackAnalysisAdditionalJson(
			long timeSpan,
			float averageSpeed,
			float maxSpeed,
			float averageSensorSpeed,
			float maxSensorSpeed,
			float averageSensorHeartRate,
			int maxSensorHeartRate,
			float averageSensorCadence,
			float maxSensorCadence,
			float averageSensorPower,
			int maxSensorPower,
			float averageSensorTemperature,
			int maxSensorTemperature,
			double diffElevationUp,
			double diffElevationDown,
			double averageElevation,
			double maxElevation
	) {
		public static TrackAnalysisAdditionalJson from(GpxTrackAnalysis analysis) {
			return new TrackAnalysisAdditionalJson(
					analysis.getTimeSpan(),
					analysis.getAvgSpeed(),
					analysis.getMaxSpeed(),
					analysis.getAvgSensorSpeed(),
					analysis.getMaxSensorSpeed(),
					analysis.getAvgSensorHr(),
					analysis.getMaxSensorHr(),
					analysis.getAvgSensorCadence(),
					analysis.getMaxSensorCadence(),
					analysis.getAvgSensorPower(),
					analysis.getMaxSensorPower(),
					analysis.getAvgSensorTemperature(),
					analysis.getMaxSensorTemperature(),
					analysis.getDiffElevationUp(),
					analysis.getDiffElevationDown(),
					analysis.getAvgElevation(),
					analysis.getMaxElevation());
		}

		public void applyTo(GpxTrackAnalysis analysis) {
			if (analysis == null) {
				return;
			}
			analysis.setTimeSpan(timeSpan);
			analysis.setAvgSpeed(averageSpeed);
			analysis.setMaxSpeed(maxSpeed);
			analysis.setAvgSensorSpeed(averageSensorSpeed);
			analysis.setMaxSensorSpeed(maxSensorSpeed);
			analysis.setAvgSensorHr(averageSensorHeartRate);
			analysis.setMaxSensorHr(maxSensorHeartRate);
			analysis.setAvgSensorCadence(averageSensorCadence);
			analysis.setMaxSensorCadence(maxSensorCadence);
			analysis.setAvgSensorPower(averageSensorPower);
			analysis.setMaxSensorPower(maxSensorPower);
			analysis.setAvgSensorTemperature(averageSensorTemperature);
			analysis.setMaxSensorTemperature(maxSensorTemperature);
			analysis.setDiffElevationUp(diffElevationUp);
			analysis.setDiffElevationDown(diffElevationDown);
			analysis.setAvgElevation(averageElevation);
			analysis.setMaxElevation(maxElevation);
		}
	}

	private record TrackAnalysisSplit(TrackAnalysisJson core, TrackAnalysisAdditionalJson additional) {
	}

	private static TrackAnalysisSplit getAnalysisFromGpx(GpxTrackAnalysis analysis) {
		if (analysis == null) {
			return null;
		}
		analysis.getPointAttributes().clear();
		return new TrackAnalysisSplit(TrackAnalysisJson.from(analysis), TrackAnalysisAdditionalJson.from(analysis));
	}

	public ResponseEntity<String> refreshListFiles(List<UserFileUpdate> files,
	                                               CloudUserDevicesRepository.CloudUserDevice dev) {
		Map<String, Set<String>> sharedFilesMap = shareFileService.getFilesByOwner(dev.userid);
		List<CloudUserFilesRepository.UserFileNoData> result = new ArrayList<>();
		for (UserFileUpdate file : files) {
			if (skipByTimeOrError(file)) {
				continue;
			}
			CloudUserFilesRepository.UserFileNoData nd = getUniqueUserFileNoData(dev, file);
			if (nd == null) {
				continue;
			}
			Optional<CloudUserFilesRepository.UserFile> of = userFilesRepository.findById(nd.id);
			boolean isTrack = FILE_TYPE_GPX.equals(file.type);
			if (of.isPresent()) {
				GpxTrackAnalysis analysis = null;
				GpxFile gpxFile;
				List<WptPt> points = null;
				CloudUserFilesRepository.UserFile uf = of.get();
				try (InputStream in = uf.data != null
						? new ByteArrayInputStream(uf.data)
						: userdataService.getInputStream(uf)) {
					if (uf.name.endsWith(INFO_FILE_SUFFIX)) {
						processInfoFile(in, uf, nd, result);
						continue;
					}
					try (GZIPInputStream gzipIn = new GZIPInputStream(in);
					     Source source = new Buffer().readFrom(gzipIn)) {
						gpxFile = GpxUtilities.INSTANCE.loadGpxFile(source);
					} catch (IOException e) {
						logAndSaveError("load-gpx-error " + e.getMessage(), uf, nd, result);
						continue;
					}
					if (gpxFile.getError() != null) {
						logAndSaveError("corrupted-gpx-file " + gpxFile.getError().getMessage(), uf, nd, result);
						continue;
					}
					if (isTrack) {
						analysis = getAnalysis(uf, gpxFile);
						points = gpxFile.getAllSegmentsPoints();
					}
					boolean isSharedFile = isShared(nd, sharedFilesMap);
					JsonObject newDetails = preparedDetails(gpxFile, analysis, isTrack, isSharedFile);
					saveDetails(newDetails, ANALYSIS, uf, points);
					nd.details = detailsForResponse(uf.details);
					result.add(nd);
				} catch (IOException e) {
					logAndSaveError("input-stream-error " + e.getMessage(), uf, nd, result);
				}
			}
		}
		return ResponseEntity.ok(gson.toJson(result));
	}

	private CloudUserFilesRepository.UserFileNoData getUniqueUserFileNoData(
			CloudUserDevicesRepository.CloudUserDevice dev, UserFileUpdate file) {
		Set<String> types = file.type != null ? Set.of(file.type) : Collections.emptySet();
		UserFilesResults res = userdataService.generateFiles(dev.userid, file.name, false, true, types);
		if (res.uniqueFiles.isEmpty()) {
			LOG.error(String.format("refreshListFiles error: no files found for %s", file.name));
			return null;
		}
		if (res.uniqueFiles.size() > 1) {
			LOG.error(String.format("refreshListFiles error: expected a single file, but got %d files",
					res.uniqueFiles.size()));
			return null;
		}
		return res.uniqueFiles.get(0);
	}

	private void logAndSaveError(String error, CloudUserFilesRepository.UserFile uf,
	                             CloudUserFilesRepository.UserFileNoData nd,
	                             List<CloudUserFilesRepository.UserFileNoData> result) {
		String errorMessage = String.format("refreshListFiles error: %s %s id=%d userid=%d",
				error, uf.name, uf.id, uf.userid);
		LOG.error(errorMessage);
		saveError(uf.details, errorMessage, uf);
		nd.details = detailsForResponse(uf.details);
		result.add(nd);
	}

	private void processInfoFile(InputStream in, CloudUserFilesRepository.UserFile uf,
	                             CloudUserFilesRepository.UserFileNoData nd,
	                             List<CloudUserFilesRepository.UserFileNoData> result) {
		JsonElement infoDetails = getInfoDetails(in);
		if (infoDetails != null) {
			if (uf.details == null) {
				uf.details = new JsonObject();
			}
			uf.details.addProperty(UPDATETIME, nd.updatetimems);
			uf.details.add(INFO_DATA_JSON, infoDetails);
			userFilesRepository.save(uf);
			nd.details = uf.details;
			result.add(nd);
		}
	}

	private boolean skipByTimeOrError(UserFileUpdate file) {
		if (file.isError) {
			if (file.time == null) {
				LOG.warn(String.format("Skipping file %s: isError=true and time is null", file.name));
				return true;
			}
			long time = Long.parseLong(file.time);
			if (System.currentTimeMillis() - time > ERROR_LIFETIME) {
				LOG.warn(String.format("Skipping file %s: isError=true and time exceeded ERROR_LIFETIME", file.name));
				return true;
			}
		}
		return false;
	}

	public JsonObject detailsForResponse(JsonObject details) {
		JsonObject response = new JsonObject();
		for (String key : details.keySet()) {
			if (!ANALYSIS_ADDITIONAL.equals(key)) {
				response.add(key, details.get(key));
			}
		}
		return response;
	}

	public JsonElement getInfoDetails(InputStream inputStream) {
		try (InputStream is = new GZIPInputStream(inputStream);
		     Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
			return gson.fromJson(reader, JsonElement.class);
		} catch (Exception e) {
			LOG.warn("Failed to parse .info details JSON", e);
			return null;
		}
	}

	public JsonObject preparedDetails(GpxFile gpxFile, GpxTrackAnalysis analysis, boolean isTrack, boolean isShared) {
		JsonObject details = new JsonObject();
		if (gpxFile != null) {
			addMetadata(details, gpxFile);
			if (isTrack) {
				addTrackData(details, analysis);
				String author = gpxFile.getAuthor();
				if (author != null && !author.isBlank()) {
					if (author.length() > 100) {
						author = author.substring(0, 100) + "...";
					}
					details.addProperty(AUTHOR, author);
				}
				if (!gpxFile.getRoutes().isEmpty()) {
					details.addProperty(HAS_ADVANCED_ROUTE, true);
				}
			}
		}
		details.add(SHARE, gson.toJsonTree(isShared));
		details.addProperty(UPDATETIME, System.currentTimeMillis());

		return details;
	}

	public boolean detailsPresent(JsonObject details) {
		return details != null
				&& details.has(UPDATETIME)
				&& (!details.has(ERROR_DETAILS) || detailsErrorWasChecked(details))
				&& details.get(UPDATETIME).getAsLong() >= ANALYSIS_RERUN;
	}

	public boolean detailsErrorWasChecked(JsonObject details) {
		return details != null
				&& details.has(ERROR_DETAILS)
				&& details.has(UPDATETIME)
				&& System.currentTimeMillis() - details.get(UPDATETIME).getAsLong() < ERROR_LIFETIME;
	}

	public boolean detailsInfoPresent(JsonObject details, long updatetimems) {
		return details != null
				&& details.has(UPDATETIME)
				&& details.has(INFO_DATA_JSON)
				&& details.get(UPDATETIME).getAsLong() == updatetimems;
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

	public boolean isShared(CloudUserFilesRepository.UserFileNoData file, Map<String, Set<String>> sharedFilesMap) {
		Set<String> types = sharedFilesMap.get(file.name);
		return types != null && types.contains(file.type);
	}

	public void addDeviceInformation(CloudUserFilesRepository.UserFileNoData file, Map<Integer, String> devices) {
		String deviceInfo = devices.get(file.deviceid);
		if (deviceInfo == null) {
			CloudUserDevicesRepository.CloudUserDevice device = userDevicesRepository.findById(file.deviceid);
			if (device != null && device.brand != null && device.model != null) {
				deviceInfo = device.brand + "__model__" + device.model;
				devices.put(file.deviceid, deviceInfo);
			}
		}
		file.deviceInfo = (deviceInfo);
	}

	public boolean analysisPresent(String tag, CloudUserFilesRepository.UserFile userFile) {
		if (userFile == null) {
			return false;
		}
		JsonObject details = userFile.details;
		return analysisPresent(tag, details);
	}

	public void saveAnalysis(String tag, CloudUserFilesRepository.UserFile file, GpxTrackAnalysis analysis) {
		if (analysis != null) {
			if (file.details == null) {
				file.details = new JsonObject();
			}
			TrackAnalysisSplit split = getAnalysisFromGpx(analysis);
			file.details.add(tag, gsonWithNans.toJsonTree(split.core()));
			if (ANALYSIS.equals(tag)) {
				file.details.add(ANALYSIS_ADDITIONAL, gsonWithNans.toJsonTree(split.additional()));
			}
		}
		saveDetails(file.details, tag, file, null);
	}

	public void saveDetails(JsonObject newDetails, String tag, CloudUserFilesRepository.UserFile file, List<WptPt> points) {
		newDetails.addProperty(tag + DONE_SUFFIX, System.currentTimeMillis());
		file.details = newDetails;

		if (points != null) {
			file.shortlinktiles = trackAnalyzerService.getQuadTileShortlinks(points);
		}

		userFilesRepository.save(file);
	}

	public GpxTrackAnalysis getAnalysis(CloudUserFilesRepository.UserFile file, GpxFile gpxFile) {
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

	private void addTrackData(JsonObject details, GpxTrackAnalysis analysis) {
		if (analysis != null) {
			TrackAnalysisSplit split = getAnalysisFromGpx(analysis);
			details.add(ANALYSIS, gsonWithNans.toJsonTree(split.core()));
			details.add(ANALYSIS_ADDITIONAL, gsonWithNans.toJsonTree(split.additional()));
		}
	}

	private void saveError(JsonObject details, String error, CloudUserFilesRepository.UserFile uf) {
		if (details == null) {
			details = new JsonObject();
		}
		details.addProperty(ERROR_DETAILS, error);
		details.addProperty(UPDATETIME, System.currentTimeMillis());
		uf.details = details;

		userFilesRepository.save(uf);
	}

	private boolean isHidden(WebGpxParser.WebPointsGroup group) {
		if (group.hidden != null) {
			return group.hidden;
		}
		for (WebGpxParser.Wpt wpt : group.points) {
			if (wpt.hidden != null && wpt.hidden.equals("true")) {
				return true;
			}
		}
		return false;
	}

	GpxTrackAnalysis getAnalysisFromJson(JsonObject details) {
		GpxTrackAnalysis analysis = new GpxTrackAnalysis();
		if (details == null || !analysisPresent(ANALYSIS, details)) {
			return analysis;
		}
		JsonElement analysisElem = details.get(ANALYSIS);
		if (analysisElem != null && analysisElem.isJsonObject()) {
			TrackAnalysisJson core = gsonWithNans.fromJson(analysisElem, TrackAnalysisJson.class);
			core.applyTo(analysis);
			JsonElement additionalElem = details.get(ANALYSIS_ADDITIONAL);
			if (additionalElem != null && additionalElem.isJsonObject()) {
				TrackAnalysisAdditionalJson additional = gsonWithNans.fromJson(additionalElem, TrackAnalysisAdditionalJson.class);
				additional.applyTo(analysis);
			}
		}
		return analysis;
	}

	@Transactional
	public ResponseEntity<String> renameFile(String oldName, String newName, String type, CloudUserDevicesRepository.CloudUserDevice dev,
	                                         boolean saveCopy, HttpSession session) throws IOException {
		CloudUserFilesRepository.UserFile file = userdataService.getLastFileVersion(dev.userid, oldName, type);
		if (file != null && file.filesize != -1) {
			File updatedFile = renameGpxTrack(file, newName);
			StorageService.InternalZipFile zipFile = null;
			if (updatedFile != null) {
				sessionResources.addGpxTempFilesToSession(session, updatedFile);
				zipFile = StorageService.InternalZipFile.buildFromFileAndDelete(updatedFile);
			}
			if (zipFile == null) {
				zipFile = userdataService.getZipFile(file, newName, session);
			}
			if (zipFile != null) {
				try {
					userdataService.validateUserForUpload(dev, type, zipFile.getSize());
				} catch (OsmAndPublicApiException e) {
					return ResponseEntity.badRequest().body(e.getMessage());
				}
				ResponseEntity<String> res = userdataService.uploadFile(zipFile, dev, newName, type, System.currentTimeMillis());
				if (res.getStatusCode().is2xxSuccessful()) {
					boolean renamed = renameInfoFile(oldName, newName, dev, saveCopy, session);
					if (!renamed) {
						return ResponseEntity.badRequest().body("Error rename info file!");
					}
					if (!saveCopy) {
						//delete file with old name
						userdataService.deleteFile(oldName, type, null, null, dev);
					}
					return userdataService.ok();
				}
			} else {
				// skip deleted files
				return userdataService.ok();
			}
		}
		return ResponseEntity.badRequest().body(saveCopy ? "Error create duplicate file!" : "Error rename file!");
	}

	private String prepareFileName(String fileName) {
		if (fileName == null || fileName.isEmpty()) {
			return null;
		}
		int lastDot = fileName.lastIndexOf('.');
		String name = lastDot > 0 ? fileName.substring(0, lastDot) : fileName;

		return name.substring(name.lastIndexOf('/') + 1);
	}

	private File renameGpxTrack(CloudUserFilesRepository.UserFile file, String newName) throws IOException {
		String preparedName = prepareFileName(newName);
		if (preparedName == null) {
			return null;
		}
		boolean isTrack = file.type.equals(FILE_TYPE_GPX);
		if (isTrack) {
			GpxFile gpxFile = null;
			InputStream in = null;
			try {
				in = file.data != null ? new ByteArrayInputStream(file.data) : userdataService.getInputStream(file);
			} catch (Exception e) {
				String isError = String.format(
						"renameFile error: input-stream-error %s id=%d userid=%d error (%s)",
						file.name, file.id, file.userid, e.getMessage());
				LOG.error(isError);
			}
			if (in != null) {
				in = new GZIPInputStream(in);
				try (Source source = new Buffer().readFrom(in)) {
					gpxFile = GpxUtilities.INSTANCE.loadGpxFile(source);
				} catch (IOException e) {
					String loadError = String.format(
							"renameFile error: load-gpx-error %s id=%d userid=%d error (%s)",
							file.name, file.id, file.userid, e.getMessage());
					LOG.error(loadError);
				}
				if (gpxFile != null && gpxFile.getError() != null) {
					String corruptedError = String.format(
							"renameFile error: corrupted-gpx-file %s id=%d userid=%d error (%s)",
							file.name, file.id, file.userid, gpxFile.getError().getMessage());
					LOG.error(corruptedError);
				}
			} else {
				String noIsError = String.format(
						"renameFile error: no-input-stream %s id=%d userid=%d", file.name, file.id, file.userid);
				LOG.error(noIsError);
			}
			if (gpxFile != null) {
				gpxFile.updateTrackName(preparedName);
				File tmpGpx = File.createTempFile(GPX_FILE_PREFIX + preparedName.replace("/../", "/"), ".gpx");
				Exception exception = GpxUtilities.INSTANCE.writeGpxFile(new KFile(tmpGpx.getAbsolutePath()), gpxFile);
				if (exception != null) {
					String isError = String.format(
							"renameFile error: write-gpx-error %s id=%d userid=%d error (%s)",
							file.name, file.id, file.userid, exception.getMessage());
					LOG.error(isError);
				} else  {
					return tmpGpx;
				}
			}
		}
		return null;
	}

	private boolean renameInfoFile(String oldName, String newName, CloudUserDevicesRepository.CloudUserDevice dev, boolean saveCopy,
	                               HttpSession session) throws IOException {
		CloudUserFilesRepository.UserFile file = userdataService.getLastFileVersion(dev.userid, oldName + INFO_FILE_EXT, FILE_TYPE_GPX);
		if (file == null || file.filesize == -1) {
			return true;
		}
		InputStream in;
		try {
			in = file.data != null ? new ByteArrayInputStream(file.data) : userdataService.getInputStream(file);
		} catch (Exception e) {
			String isError = String.format(
					"renameInfoFile error: input-stream-error %s id=%d userid=%d error (%s)",
					file.name, file.id, file.userid, e.getMessage());
			LOG.error(isError);
			return false;
		}
		if (in == null) {
			LOG.error(String.format(
					"renameInfoFile error: no-input-stream %s id=%d userid=%d",
					file.name, file.id, file.userid
			));
			return false;
		}
		try (GZIPInputStream gis = new GZIPInputStream(in)) {
			// read json
			ObjectMapper mapper = new ObjectMapper();

			ObjectNode json = (ObjectNode) mapper.readTree(gis);
			String oldPath = json.path("file").asText();
			// save /tracks folder
			int tracksFolderEndIndex = oldPath.indexOf('/', oldPath.startsWith("/") ? 1 : 0);
			String folder = (tracksFolderEndIndex >= 0)
					? oldPath.substring(0, tracksFolderEndIndex + 1)
					: "";

			// save new name
			json.put("file", folder + newName);
			// prepare new json
			String jsonStr = mapper.writeValueAsString(json)
					.replace("/", "\\/");
			byte[] updated = jsonStr.getBytes(StandardCharsets.UTF_8);
			// create new file
			File tmpInfo = File.createTempFile(GPX_FILE_PREFIX + newName.replace("/../", "/") + INFO_FILE_EXT, INFO_FILE_EXT);
			try (FileOutputStream fos = new FileOutputStream(tmpInfo)) {
				fos.write(updated);
			}
			// upload new file
			sessionResources.addGpxTempFilesToSession(session, tmpInfo);
			StorageService.InternalZipFile zipFile = StorageService.InternalZipFile.buildFromFileAndDelete(tmpInfo);
			ResponseEntity<String> res = userdataService.uploadFile(zipFile, dev, newName + INFO_FILE_EXT, FILE_TYPE_GPX, System.currentTimeMillis());
			if (res.getStatusCode().is2xxSuccessful()) {
				if (!saveCopy) {
					userdataService.deleteFile(oldName + INFO_FILE_EXT, FILE_TYPE_GPX, null, null, dev);
				}
				return true;
			}
		}
		return false;
	}
}
