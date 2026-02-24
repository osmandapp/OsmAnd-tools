package net.osmand.server.api.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import net.osmand.obf.ToolsOsmAndContextImpl;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.shared.api.SettingsAPI;
import net.osmand.shared.gpx.*;
import net.osmand.shared.gpx.data.SmartFolder;
import net.osmand.shared.gpx.organization.OrganizeByParams;
import net.osmand.shared.io.KFile;
import net.osmand.shared.util.PlatformUtil;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static net.osmand.server.api.services.UserdataService.FILE_TYPE_GLOBAL;
import static net.osmand.server.api.services.UserdataService.FILE_TYPE_GPX;
import static net.osmand.server.api.services.WebUserdataService.*;
import static net.osmand.shared.gpx.GpxUtilities.COLOR_NAME_EXTENSION;
import static net.osmand.shared.gpx.GpxUtilities.LINE_WIDTH_EXTENSION;
import static net.osmand.shared.gpx.SmartFolderHelper.TRACK_FILTERS_SETTINGS_PREF;

@Service
public class SmartFolderService {

	public static final String GENERAL_SETTINGS_JSON_FILE = "general_settings.json";

	@Autowired
	private UserdataService userDataService;

	@Autowired
	private WebUserdataService webUserdataService;

	public List<SmartFolderWeb> getSmartFolders(int userId) {
		String trackFiltersSettings = getFiltersSettings(userId);
		if (trackFiltersSettings == null) {
			return Collections.emptyList();
		}
		PlatformUtil.INSTANCE.initialize(new OsmEmptyContext());
		List<CloudUserFilesRepository.UserFileNoData> uniqueFiles = userDataService
				.generateFiles(userId, null, false, true, Set.of(FILE_TYPE_GPX)).uniqueFiles;
		List<TrackItem> trackItems = new ArrayList<>(uniqueFiles.size());
		for (CloudUserFilesRepository.UserFileNoData uf : uniqueFiles) {
			if (!uf.name.endsWith(INFO_FILE_EXT)) {
				GpxFile gpxFile = createGpxFileWithAppearance(userId, uf);
				GpxDataItem dataItem = GpxDataItem.Companion.fromGpxFile(gpxFile, uf.name);
				dataItem.setAnalysis(webUserdataService.getAnalysis(uf.details));
				TrackItem trackItem = new TrackItem(gpxFile);
				trackItem.setDataItem(dataItem);
				trackItems.add(trackItem);
			}
		}
		synchronized (SmartFolderHelper.INSTANCE) {
			SmartFolderHelper.INSTANCE.resetSmartFolders();
			SmartFolderHelper.INSTANCE.readJson(trackFiltersSettings);
			for (TrackItem trackItem : trackItems) {
				SmartFolderHelper.INSTANCE.addTrackItemToSmartFolder(trackItem);
			}
			return toSmartFolderWebList(SmartFolderHelper.INSTANCE.getSmartFolders());
		}
	}

	private List<SmartFolderWeb> toSmartFolderWebList(List<SmartFolder> smartFolders) {
		List<SmartFolderWeb> smartFolderList = new ArrayList<>();
		for (SmartFolder smartFolder : smartFolders) {
			String name = smartFolder.getFolderName();
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
			SmartFolderWeb smartFolderWeb = new SmartFolderWeb(name, organizeBy, userFilePaths);
			smartFolderList.add(smartFolderWeb);
		}
		return smartFolderList;
	}

	private GpxFile createGpxFileWithAppearance(int userId, CloudUserFilesRepository.UserFileNoData uf) {
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
		setAppearance(gpxFile, uf.name, userId);
		return gpxFile;
	}

	private String getFiltersSettings(int userId) {
		String generalSettings = getGeneralSettings(userId);
		if (generalSettings == null) {
			return null;
		}
		JSONObject obj = new JSONObject(generalSettings);
		return obj.optString(TRACK_FILTERS_SETTINGS_PREF, null);
	}

	void setAppearance(GpxFile gpxFile, String name, int userId) {
		CloudUserFilesRepository.UserFile file = userDataService.getLastFileVersion(userId, name + INFO_FILE_EXT, FILE_TYPE_GPX);
		if (file == null || file.filesize == -1 || file.data == null) {
			return;
		}
		try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(file.data))) {
			ObjectMapper mapper = new ObjectMapper();

			ObjectNode json = (ObjectNode) mapper.readTree(gis);
			JsonNode color = json.get(COLOR_NAME_EXTENSION);
			if (color != null) {
				gpxFile.setColor(color.asText());
			}
			JsonNode width = json.get(LINE_WIDTH_EXTENSION);
			if (width != null) {
				gpxFile.setWidth(width.asText());
			}
		} catch (Exception e) {
			String isError = String.format("ReadInfoFile error: input-stream-error %s id=%d userid=%d error (%s)",
					file.name, file.id, file.userid, e.getMessage());
			LOG.error(isError);
		}
	}

	private String getGeneralSettings(int userId) {
		String generalSettings = null;
		CloudUserFilesRepository.UserFile userFile = userDataService.getLastFileVersion(userId, GENERAL_SETTINGS_JSON_FILE, FILE_TYPE_GLOBAL);
		if (userFile != null) {
			try (InputStream is = new GZIPInputStream(new ByteArrayInputStream(userFile.data))) {
				generalSettings = new String(is.readAllBytes(), StandardCharsets.UTF_8);
			} catch (IOException e) {
				LOG.error(String.format("Read GeneralSettings error: (%s)", e.getMessage()));
			}
		}
		return generalSettings;
	}

	public record SmartFolderWeb(String name, String organizeBy, List<String> userFilePaths) {
	}

	static class OsmEmptyContext extends ToolsOsmAndContextImpl {
		@Override
		public SettingsAPI getSettings() {
			return null;
		}

		@Override
		public KFile getGpxDir() {
			return new KFile("");
		}
	}

}
