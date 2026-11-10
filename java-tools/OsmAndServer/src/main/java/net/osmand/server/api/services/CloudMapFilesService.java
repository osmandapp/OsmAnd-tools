package net.osmand.server.api.services;

import static net.osmand.server.api.services.UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE;
import static net.osmand.server.api.services.UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE;
import static net.osmand.server.api.services.UserdataService.FILE_TYPE;
import static net.osmand.server.api.services.UserdataService.INFO_EXT;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.xml.bind.JAXB;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

import net.osmand.IndexConstants;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.services.DownloadIndexesService.DownloadType;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;
import net.osmand.server.utils.exception.OsmAndPublicApiException;

/** Keeps obsolete standard maps out of Cloud transfers. */
@Service
public class CloudMapFilesService {

	private static final String OUTDATED_DELETED_MAPS_XML = "/countries-info/outdated-deleted-maps.xml";

	@Autowired
	private CloudUserFilesRepository filesRepository;

	private final Gson gson = new Gson();
	private final Set<String> deletedMapNames = createDeletedMapNames();

	public static void loadDeletedMaps(List<DownloadIndex> deletedMaps) {
		try (InputStream input = Objects.requireNonNull(
				CloudMapFilesService.class.getResourceAsStream(OUTDATED_DELETED_MAPS_XML), OUTDATED_DELETED_MAPS_XML)) {
			DownloadIndexDocument document = JAXB.unmarshal(input, DownloadIndexDocument.class);
			for (DownloadIndex map : document.getDeletedMaps()) {
				map.setDateByString(map.getDate());
				map.setType(DownloadType.DELETED_MAP);
				deletedMaps.add(map);
			}
		} catch (IOException | ParseException e) {
			throw new IllegalStateException("Failed to read " + OUTDATED_DELETED_MAPS_XML, e);
		}
	}

	public boolean isDeletedMapFile(String name, String type) {
		if (!FILE_TYPE.equalsIgnoreCase(type)) {
			return false;
		}
		if (name.endsWith(INFO_EXT)) {
			name = name.substring(0, name.length() - INFO_EXT.length());
		}
		if (!name.endsWith(IndexConstants.BINARY_MAP_INDEX_EXT)) {
			return false;
		}
		return deletedMapNames.contains(DownloadIndexesService.toIndexFileName(name));
	}

	public void prepareFileListForDeletedMaps(UserFilesResults result, boolean removeFromList) {
		for (Iterator<UserFileNoData> iterator = result.uniqueFiles.iterator(); iterator.hasNext();) {
			UserFileNoData file = iterator.next();
			if (isDeletedMapFile(file.name, file.type)) {
				iterator.remove();
			}
		}
		if (result.allFiles == null) {
			return;
		}
		Set<String> marked = new HashSet<>();
		for (Iterator<UserFileNoData> iterator = result.allFiles.iterator(); iterator.hasNext();) {
			UserFileNoData file = iterator.next();
			if (isDeletedMapFile(file.name, file.type)) {
				if (removeFromList || file.name.endsWith(INFO_EXT) || !marked.add(file.type + "/" + file.name)) {
					iterator.remove();
				} else {
					// Negative size prevents downloads; zero update time prevents local deletion.
					file.filesize = -1;
					file.zipSize = 0;
					file.updatetime = new Date(0);
					file.updatetimems = 0;
				}
			}
		}
	}

	public ResponseEntity<String> handleDeletedMapUpload(String name, String type, Long clienttime, CloudUserDevice dev) {
		// Acknowledge obsolete maps without storing their content.
		long uploadTime = Math.max(System.currentTimeMillis(), clienttime == null ? 0 : clienttime);
		if (!name.endsWith(INFO_EXT)) {
			UserFile previous = filesRepository.findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(dev.userid, name, type);
			if (previous == null || previous.filesize == null || previous.filesize >= 0) {
				UserFile marker = new UserFile();
				marker.userid = dev.userid;
				marker.deviceid = dev.id;
				marker.name = name;
				marker.type = type;
				marker.filesize = -1L;
				marker.zipfilesize = 0L;
				marker.updatetime = new Date(uploadTime);
				marker.clienttime = clienttime == null ? null : new Date(clienttime);
				filesRepository.saveAndFlush(marker);
			}
		}
		return ResponseEntity.ok(gson.toJson(Map.of("status", "ok", "updatetime", uploadTime, "filesize", 0)));
	}

	public void validateDownload(String name, String type) {
		if (isDeletedMapFile(name, type)) {
			throw new OsmAndPublicApiException(ERROR_CODE_FILE_NOT_AVAILABLE, ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
		}
	}

	public boolean isDeletedMapMarker(String name, String type, long updatetime) {
		// Version cleanup must keep the synthetic deletion marker.
		return updatetime == 0 && isDeletedMapFile(name, type);
	}

	private static Set<String> createDeletedMapNames() {
		List<DownloadIndex> deletedMaps = new ArrayList<>();
		loadDeletedMaps(deletedMaps);
		Set<String> names = new HashSet<>();
		for (DownloadIndex index : deletedMaps) {
			String name = index.getName();
			names.add(name.endsWith(IndexConstants.ZIP_EXT)
					? name.substring(0, name.length() - IndexConstants.ZIP_EXT.length()) : name);
		}
		return names;
	}
}
