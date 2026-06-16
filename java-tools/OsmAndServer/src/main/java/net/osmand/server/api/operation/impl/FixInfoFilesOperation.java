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

/**
 * Backfills required tags (type/file/subtype) into GPX *.info files that only contain "pointsGroups".
 */
@Component
@AdminOperation(name = "fix-info-files")
public class FixInfoFilesOperation extends AbstractFileFixOperation {

	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final String INFO_EXT = ".info";
	private static final String GPX_INFO_EXT = ".gpx.info";
	private static final String POINTS_GROUPS = "pointsGroups";
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
	protected byte[] processFile(UserFile file) throws IOException {
		if (file.name == null || !file.name.endsWith(GPX_INFO_EXT)) {
			return null;
		}
		ObjectNode fixed = fix(file);
		return fixed == null ? null : MAPPER.writeValueAsBytes(fixed);
	}

	@Override
	protected ObjectNode fix(UserFile file) throws IOException {
		JsonNode node = MAPPER.readTree(read(file));
		if (node == null || !node.isObject()) {
			return null;
		}
		ObjectNode obj = (ObjectNode) node;
		if (obj.size() == 1 && obj.has(POINTS_GROUPS)) {
			ObjectNode out = MAPPER.createObjectNode();
			out.put(KEY_TYPE, TYPE_GPX);
			out.put(KEY_FILE, TRACKS_PREFIX + baseName(file.name));
			out.put(KEY_SUBTYPE, SUBTYPE_GPX);
			out.setAll(obj);
			return out;
		}
		return null;
	}

	static String baseName(String name) {
		String n = name.endsWith(INFO_EXT) ? name.substring(0, name.length() - INFO_EXT.length()) : name;
		int slash = n.lastIndexOf('/');
		return slash >= 0 ? n.substring(slash + 1) : n;
	}
}
