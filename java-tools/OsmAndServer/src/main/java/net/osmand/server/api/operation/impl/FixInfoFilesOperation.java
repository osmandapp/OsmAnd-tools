package net.osmand.server.api.operation.impl;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.UserdataService;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Repairs the required tags (type/file/subtype) of GPX *.info files.
 * <p>
 * "file" is the track path on the device, so it must be "/tracks/" + the cloud name of the gpx,
 * folder included. Two cases are fixed: the tags are missing altogether (old web uploads that
 * contained only "pointsGroups"), and "file" points at a wrong path - web used to build it from the
 * track title, dropping the folder and the extension, so Android could not match the item.
 */
@Component
@AdminOperation(name = "fix-info-files")
public class FixInfoFilesOperation extends AbstractFileFixOperation {

	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final String INFO_EXT = ".info";
	private static final String GPX_INFO_EXT = ".gpx.info";
	private static final String KEY_TYPE = "type";
	private static final String KEY_FILE = "file";
	private static final String KEY_SUBTYPE = "subtype";
	private static final String TYPE_GPX = "GPX";
	private static final String SUBTYPE_GPX = "gpx";
	private static final String TRACKS_PREFIX = "/tracks/";

	public FixInfoFilesOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
								 UserdataService userdataService, StorageService storageService) {
		super(usersRepository, filesRepository, userdataService, storageService);
	}

	@Override
	public Set<String> supportedTypes() {
		return Set.of(UserdataService.FILE_TYPE_GPX);
	}

	@Override
	protected boolean accepts(String name) {
		return name != null && name.endsWith(GPX_INFO_EXT);
	}

	@Override
	protected boolean fix(UserFile file, Params params) throws IOException {
		JsonNode node = MAPPER.readTree(read(file));
		if (node == null || !node.isObject()) {
			return false;
		}
		ObjectNode obj = (ObjectNode) node;
		String expectedFile = TRACKS_PREFIX + trackName(file.name);
		boolean noFile = !obj.hasNonNull(KEY_FILE);
		boolean wrongFile = !noFile && !expectedFile.equals(obj.get(KEY_FILE).asText());
		if (!noFile && !wrongFile) {
			return false;
		}
		ObjectNode rest = obj.deepCopy();
		rest.remove(List.of(KEY_TYPE, KEY_FILE, KEY_SUBTYPE));
		ObjectNode out = MAPPER.createObjectNode();
		out.put(KEY_TYPE, TYPE_GPX);
		out.put(KEY_FILE, expectedFile);
		out.put(KEY_SUBTYPE, SUBTYPE_GPX);
		out.setAll(rest);
		if (!isTest(params)) {
			save(file, MAPPER.writeValueAsBytes(out));
		}
		return true;
	}

	// cloud name of the gpx the info file belongs to, folder included: "Folder/Track.gpx.info" -> "Folder/Track.gpx"
	static String trackName(String name) {
		return name.endsWith(INFO_EXT) ? name.substring(0, name.length() - INFO_EXT.length()) : name;
	}
}
