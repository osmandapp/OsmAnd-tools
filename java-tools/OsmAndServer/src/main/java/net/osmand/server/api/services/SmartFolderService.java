package net.osmand.server.api.services;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import net.osmand.shared.gpx.*;
import net.osmand.shared.gpx.data.SmartFolder;
import net.osmand.shared.gpx.organization.OrganizeByParams;
import net.osmand.shared.io.KFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static net.osmand.server.api.repo.CloudUserFilesRepository.*;
import static net.osmand.server.api.services.UserdataService.FILE_TYPE_GLOBAL;
import static net.osmand.server.api.services.UserdataService.FILE_TYPE_GPX;
import static net.osmand.server.api.services.WebUserdataService.*;
import static net.osmand.shared.gpx.GpxUtilities.COLOR_NAME_EXTENSION;
import static net.osmand.shared.gpx.GpxUtilities.LINE_WIDTH_EXTENSION;
import static net.osmand.shared.gpx.SmartFolderHelper.TRACK_FILTERS_SETTINGS_PREF;

@Service
public class SmartFolderService {

	private static final Log LOG = LogFactory.getLog(SmartFolderService.class);

	public static final String GENERAL_SETTINGS_JSON_FILE = "general_settings.json";

	@Autowired
	private UserdataService userDataService;

	@Autowired
	private WebUserdataService webUserdataService;

	public List<SmartFolderWeb> getFilteredSmartFolders(int userId) {
		String trackFiltersSettings = getFiltersSettings(userId);
		if (trackFiltersSettings == null) {
			return new ArrayList<>();
		}
		List<UserFileNoData> uniqueFiles = userDataService
				.generateFiles(userId, null, false, true, Set.of(FILE_TYPE_GPX)).uniqueFiles;
		List<TrackItem> trackItems = new ArrayList<>(uniqueFiles.size());
		for (UserFileNoData uf : uniqueFiles) {
			if (!uf.name.endsWith(INFO_FILE_EXT)) {
				GpxDataItem dataItem = new GpxDataItem(new KFile(""));
				GpxFile gpxFile = createGpxFileWithAppearance(userId, uf);
				dataItem.readGpxParams(gpxFile);
				dataItem.setAnalysis(webUserdataService.getAnalysisFromJson(uf.details));
				TrackItem trackItem = new TrackItem(gpxFile);
				trackItem.setDataItem(dataItem);
				trackItems.add(trackItem);
			}
		}
		SmartFolderHelper smartFolderHelper = new SmartFolderHelper();
		smartFolderHelper.readJson(trackFiltersSettings);
		for (TrackItem trackItem : trackItems) {
			smartFolderHelper.addTrackItemToSmartFolder(trackItem);
		}
		return toSmartFolderWebList(smartFolderHelper.getSmartFolders());
	}

	private List<SmartFolderWeb> toSmartFolderWebList(List<SmartFolder> smartFolders) {
		List<SmartFolderWeb> smartFolderList = new ArrayList<>();
		for (SmartFolder smartFolder : smartFolders) {
			OrganizeByParams organizeByParams = smartFolder.getOrganizeByParams();
			String organizeBy = null;
			if (organizeByParams != null) {
				organizeBy = organizeByParams.getType().getName();
			}
			List<String> userFilePaths = new ArrayList<>();
			for (TrackItem trackItem : smartFolder.getTrackItems()) {
				KFile file = trackItem.getFile();
				if (file != null) {
					userFilePaths.add(file.path());
				}
			}
			String name = smartFolder.getFolderName();
			long creationTime = smartFolder.getCreationTime();
			SmartFolderWeb smartFolderWeb = new SmartFolderWeb(name, organizeBy, userFilePaths, creationTime);
			smartFolderList.add(smartFolderWeb);
		}
		return smartFolderList;
	}

	private GpxFile createGpxFileWithAppearance(int userId, UserFileNoData uf) {
		GpxFile gpxFile = new GpxFile(null);
		gpxFile.setPath(uf.name);
		gpxFile.setModifiedTime(uf.clienttime.getTime());
		JsonObject details = uf.details;
		if (details != null) {
			JsonObject metadata = details.getAsJsonObject(METADATA);
			if (metadata != null) {
				JsonElement time = metadata.get("time");
				if (time != null) {
					gpxFile.getMetadata().setTime(time.getAsLong());
				}
			}
		}
		UserFile infoFile = userDataService.getLastFileVersion(userId, uf.name + INFO_FILE_EXT, FILE_TYPE_GPX);
		if (infoFile != null && infoFile.details != null) {
			setAppearanceFromJson(gpxFile, infoFile.details);
		}
		return gpxFile;
	}

	private String getFiltersSettings(int userId) {
		String generalSettings = getGeneralSettings(userId);
		if (generalSettings == null) {
			return null;
		}
		JSONObject obj = new JSONObject(generalSettings);
		return obj.has(TRACK_FILTERS_SETTINGS_PREF)
				? obj.optString(TRACK_FILTERS_SETTINGS_PREF, null)
				: null;
	}

	private void setAppearanceFromJson(GpxFile gpxFile, JsonObject details) {
		JsonElement color = details.get(COLOR_NAME_EXTENSION);
		if (color != null) {
			gpxFile.setColor(color.getAsString());
		}
		JsonElement width = details.get(LINE_WIDTH_EXTENSION);
		if (width != null) {
			gpxFile.setWidth(width.getAsString());
		}
	}

	private String getGeneralSettings(int userId) {
		UserFile file = userDataService.getLastFileVersion(userId, GENERAL_SETTINGS_JSON_FILE, FILE_TYPE_GLOBAL);
		if (file == null) {
			return null;
		}
		try (InputStream in = getInputStreamFromFile(file)) {
			if (in == null) {
				return null;
			}
			try (InputStream gzipStream = new GZIPInputStream(in)) {
				return new String(gzipStream.readAllBytes(), StandardCharsets.UTF_8);
			}
		} catch (IOException e) {
			LOG.error(String.format("Read GeneralSettings error for id=%d: %s", file.id, e.getMessage()));
			return null;
		}
	}

	private InputStream getInputStreamFromFile(UserFile file) {
		if (file == null) {
			return null;
		}
		return file.data != null ? new ByteArrayInputStream(file.data) : userDataService.getInputStream(file);
	}

	public record SmartFolderWeb(String name, String organizeBy, List<String> userFilePaths, long creationTime) {
	}

}
