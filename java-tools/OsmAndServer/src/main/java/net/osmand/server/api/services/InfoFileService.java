package net.osmand.server.api.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.servlet.http.HttpSession;
import net.osmand.server.controllers.pub.UserSessionResources;
import net.osmand.server.utils.exception.OsmAndPublicApiException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static net.osmand.server.api.repo.CloudUserDevicesRepository.*;
import static net.osmand.server.api.repo.CloudUserFilesRepository.*;
import static net.osmand.server.api.services.UserdataService.*;
import static net.osmand.shared.IndexConstants.GPX_FILE_EXT;
import static net.osmand.shared.IndexConstants.GPX_FILE_PREFIX;

@Service
public class InfoFileService {

	@Autowired
	UserdataService userdataService;

	@Autowired
	UserSessionResources sessionResources;

	private static final ObjectMapper MAPPER = new ObjectMapper();

	public void updateInfoFile(CloudUserDevice dev, String name, Map<String, Object> diff, HttpSession session)
			throws IOException {
		UserFile lastFileVersion = userdataService.getLastFileVersion(dev.userid, name, UserdataService.FILE_TYPE_GPX);
		ObjectNode originalJson = MAPPER.createObjectNode();
		if (lastFileVersion != null && lastFileVersion.filesize != -1) {
			originalJson = getOriginalJson(lastFileVersion);
		}
		JsonNode diffJson = MAPPER.valueToTree(diff);
		JsonNode merged = deepMerge(originalJson, diffJson);
		uploadMergedFile(dev, name, session, merged, lastFileVersion);
	}

	private ObjectNode getOriginalJson(UserFile lastFileVersion) throws IOException {
		try (InputStream in = lastFileVersion.data != null
				? new ByteArrayInputStream(lastFileVersion.data)
				: userdataService.getInputStream(lastFileVersion)) {
			if (in == null) {
				throw new OsmAndPublicApiException(ERROR_CODE_FILE_NOT_AVAILABLE, ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
			}
			try (GZIPInputStream gis = new GZIPInputStream(in)) {
				JsonNode node = MAPPER.readTree(gis);
				if (!(node instanceof ObjectNode obj)) {
					throw new OsmAndPublicApiException(ERROR_CODE_FILE_NOT_AVAILABLE,
							"Info file must be a JSON object, got: " + node.getNodeType());
				}
				return obj;
			}
		}
	}

	public static JsonNode deepMerge(ObjectNode originalJson, JsonNode diffJson) {
		if (diffJson == null || !diffJson.isObject()) {
			return originalJson;
		}
		diffJson.fields().forEachRemaining(entry -> {
			String fieldName = entry.getKey();
			JsonNode value = entry.getValue();
			JsonNode existing = originalJson.get(fieldName);
			if (value.isObject() && existing instanceof ObjectNode) {
				deepMerge((ObjectNode) existing, value);
			} else {
				originalJson.set(fieldName, value);
			}
		});
		return originalJson;
	}

	private void uploadMergedFile(CloudUserDevice dev, String name, HttpSession session, JsonNode merged,
	                              UserFile lastFileVersion) throws IOException {
		File tmpInfo = File.createTempFile(GPX_FILE_PREFIX + session.getId(), GPX_FILE_EXT);
		try (FileOutputStream fos = new FileOutputStream(tmpInfo)) {
			MAPPER.writeValue(fos, merged);
		}
		sessionResources.addGpxTempFilesToSession(session, tmpInfo);
		StorageService.InternalZipFile zipFile = StorageService.InternalZipFile.buildFromFileAndDelete(tmpInfo);
		userdataService.validateUserForUpload(dev, FILE_TYPE_GPX, zipFile.getSize());
		userdataService.uploadFile(zipFile, dev, name, FILE_TYPE_GPX, System.currentTimeMillis());
		if (lastFileVersion != null && lastFileVersion.filesize != -1) {
			long updatetime = lastFileVersion.updatetime.getTime();
			userdataService.deleteFileVersion(updatetime, dev.userid, name, FILE_TYPE_GPX, null);
		}
	}
}
