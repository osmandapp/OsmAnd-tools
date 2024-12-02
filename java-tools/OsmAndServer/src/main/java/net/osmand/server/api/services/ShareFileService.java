package net.osmand.server.api.services;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.Data;
import lombok.Getter;
import net.osmand.server.api.repo.*;
import net.osmand.server.controllers.user.ShareFileController;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import okio.Buffer;
import okio.Source;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import javax.transaction.Transactional;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static org.springframework.util.MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE;


@Service
public class ShareFileService {

	@Autowired
	protected PremiumUserFilesRepository filesRepository;

	@Autowired
	protected ShareFileRepository shareFileRepository;

	@Autowired
	protected PremiumUsersRepository usersRepository;

	@Autowired
	UserdataService userdataService;

	protected static final Log LOGGER = LogFactory.getLog(ShareFileService.class);

	private static final String SEPARATOR = ",";
	private static final String CODE_SEPARATOR = "-";
	public static final int CODE_LENGTH = 8;

	Gson gson = new Gson();

	@Transactional
	public String generateSharedCode(PremiumUserFilesRepository.UserFile userFile, String type, @Nullable String groupActions) {
		String uniqueCode = UUID.randomUUID().toString().substring(0, CODE_LENGTH);
		ShareFileRepository.ShareFile file = shareFileRepository.findByCode(uniqueCode);
		while (file != null) {
			uniqueCode = UUID.randomUUID().toString().substring(0, CODE_LENGTH);
			file = shareFileRepository.findByCode(uniqueCode);
		}
		ShareFileRepository.ShareFile shareFile = new ShareFileRepository.ShareFile();
		shareFile.setCode(uniqueCode);
		shareFile.setName(userFile.name);
		shareFile.setType(userFile.type);
		shareFile.setUserid(userFile.userid);
		shareFile.setInfo(addSharedInfo(type, groupActions));

		ShareFileRepository.ShareFile existingFile = shareFileRepository.findByUseridAndNameAndType(userFile.userid, userFile.name, userFile.type);
		if (existingFile != null) {
			shareFileRepository.delete(existingFile);
		}
		shareFileRepository.saveAndFlush(shareFile);

		return uniqueCode;
	}

	public String getNamePartForCode(String filename) {
		final String DOT = ".";
		String prefix = filename.substring(0, CODE_LENGTH);
		String suffix = filename.contains(DOT) ? filename.substring(filename.lastIndexOf(DOT) - 1) : "";
		return prefix + suffix;
	}

	public FileSharedInfo getSharedInfo(JsonObject sharedInfoJson) {
		if (sharedInfoJson != null) {
			return gson.fromJson(sharedInfoJson, FileSharedInfo.class);
		}
		return null;
	}

	public FileSharedInfo getSharedInfo(@NotNull PremiumUserDevicesRepository.PremiumUserDevice dev, @NotNull String name, @NotNull String type) {
		ShareFileRepository.ShareFile file = shareFileRepository.findByUseridAndNameAndType(dev.userid, name, type);
		if (file != null) {
			return getSharedInfo(file.getInfo());
		}
		return null;
	}

	public FileSharedInfo getSharedInfo(String code) {
		code = normalizeCode(code);
		JsonObject sharedInfoJson = shareFileRepository.findInfoByCode(code);
		if (sharedInfoJson != null) {
			return gson.fromJson(sharedInfoJson, FileSharedInfo.class);
		}
		return null;
	}

	public boolean checkAccess(FileSharedInfo info) {
		FileSharedInfo.SharingType sharingType = info.getSharingType();
		if (sharingType == FileSharedInfo.SharingType.PUBLIC) {
			return true;
		}
		PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			return false;
		}
		if (sharingType == FileSharedInfo.SharingType.PUBLIC_BY_LOGIN) {
			return true;
		}
		if (sharingType == FileSharedInfo.SharingType.GROUP_BY_LOGIN) {
			return !info.getBlacklist().getUsers().contains(dev.userid);
		}
		return false;
	}

	public ShareFileController.FileDownloadResult downloadFile(PremiumUserFilesRepository.UserFile userFile) throws IOException {
		if (userFile == null) {
			return null;
		}
		try (InputStream bin = userdataService.getInputStream(userFile)) {
			if (bin != null) {
				InputStream inputStream = new GZIPInputStream(bin);
				String fileName = URLEncoder.encode(userdataService.sanitizeEncode(userFile.name), StandardCharsets.UTF_8);
				return new ShareFileController.FileDownloadResult(inputStream, fileName, APPLICATION_OCTET_STREAM_VALUE);
			}
		}
		return null;
	}

	public ShareFileRepository.ShareFile getShareFileByCode(String code) {
		if (code == null) {
			return null;
		}
		code = normalizeCode(code);
		return shareFileRepository.findByCode(code);
	}

	public PremiumUserFilesRepository.UserFile getUserFile(ShareFileRepository.ShareFile file) {
		return filesRepository.findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(file.userid, file.name, file.type);
	}

	public GpxFile getFile(PremiumUserFilesRepository.UserFile file) throws IOException {
		if (file == null || file.data == null) {
			return null;
		}
		try (InputStream inputStream = new GZIPInputStream(new ByteArrayInputStream(file.data));
		     Source source = new Buffer().readFrom(inputStream)) {
			GpxFile gpxFile = GpxUtilities.INSTANCE.loadGpxFile(source);
			if (gpxFile.getError() == null) {
				return gpxFile;
			}
			LOGGER.error("Error loading gpx file: " + gpxFile.getError());
			return null;
		}
	}

	@Transactional
	public void storeUserAccess(PremiumUserDevicesRepository.PremiumUserDevice dev,
	                            ShareFileRepository.ShareFile shareFile,
	                            FileSharedInfo info) {
		if (info == null) {
			info = new FileSharedInfo();
		}
		Map<Integer, Long> users = info.getUsersAccessInfo().getUsers();
		if (!users.containsKey(dev.userid)) {
			users.put(dev.userid, System.currentTimeMillis());
			info.getUsersAccessInfo().setUsers(users);
			shareFile.setInfo(gson.toJsonTree(info).getAsJsonObject());
			shareFileRepository.saveAndFlush(shareFile);
		}
	}

	@Transactional
	public boolean editBlacklist(@NotNull PremiumUserDevicesRepository.PremiumUserDevice dev,
	                             @NotNull String name,
	                             @NotNull String type,
	                             List<String> list) {
		if (dev == null || name == null || type == null) {
			return false;
		}
		ShareFileRepository.ShareFile file = shareFileRepository.findByUseridAndNameAndType(dev.userid, name, type);
		if (file == null) {
			return false;
		}
		FileSharedInfo fileSharedInfo = gson.fromJson(file.getInfo(), FileSharedInfo.class);
		if (fileSharedInfo == null) {
			fileSharedInfo = new FileSharedInfo();
		}
		FileSharedInfo.Blacklist blist = fileSharedInfo.getBlacklist();
		blist.getUsers().addAll(list.stream().map(email -> {
			PremiumUsersRepository.PremiumUser pu = usersRepository.findByEmailIgnoreCase(email);
			return pu != null ? pu.id : null;
		}).filter(Objects::nonNull).toList());
		if (blist.getUsers().isEmpty()) {
			return false;
		}
		fileSharedInfo.setBlacklist(blist);
		file.setInfo(gson.toJsonTree(fileSharedInfo).getAsJsonObject());
		shareFileRepository.saveAndFlush(file);
		return true;
	}

	@Transactional
	public boolean editWhitelist(@NotNull PremiumUserDevicesRepository.PremiumUserDevice dev,
	                             @NotNull String name,
	                             @NotNull String type,
	                             List<String> list) {
		if (dev == null || name == null || type == null) {
			return false;
		}
		ShareFileRepository.ShareFile file = shareFileRepository.findByUseridAndNameAndType(dev.userid, name, type);
		if (file == null) {
			return false;
		}
		FileSharedInfo fileSharedInfo = gson.fromJson(file.getInfo(), FileSharedInfo.class);
		if (fileSharedInfo == null) {
			fileSharedInfo = new FileSharedInfo();
		}
		FileSharedInfo.Whitelist wlist = fileSharedInfo.getWhitelist();

		wlist.getPermissions().putAll(list.stream().collect(HashMap::new, (m, email) -> {
			PremiumUsersRepository.PremiumUser pu = usersRepository.findByEmailIgnoreCase(email);
			if (pu != null) {
				m.put(pu.id, new FileSharedInfo.Permission(FileSharedInfo.PermissionType.READ));
			}
		}, HashMap::putAll));
		if (wlist.getPermissions().isEmpty()) {
			return false;
		}
		fileSharedInfo.setWhitelist(wlist);
		file.setInfo(gson.toJsonTree(fileSharedInfo).getAsJsonObject());
		shareFileRepository.saveAndFlush(file);
		return true;
	}

	private JsonObject addSharedInfo(String type, @Nullable String groupActions) {
		FileSharedInfo fileSharedInfo = new FileSharedInfo();
		FileSharedInfo.SharingType sharingType = FileSharedInfo.SharingType.fromType(type);
		fileSharedInfo.setSharingType(Objects.requireNonNullElse(sharingType, FileSharedInfo.SharingType.PUBLIC));

		if (sharingType == FileSharedInfo.SharingType.GROUP_BY_LOGIN && groupActions != null) {
			String[] actions = groupActions.split(SEPARATOR);
			for (String action : actions) {
				FileSharedInfo.GroupAction groupAction = FileSharedInfo.GroupAction.fromAction(action.trim());
				if (groupAction != null) {
					fileSharedInfo.getGroupActions().add(groupAction);
				}
			}
		}

		return gson.toJsonTree(fileSharedInfo).getAsJsonObject();
	}

	private String normalizeCode(String code) {
		String normalizedCode = code;
		if (code.contains(CODE_SEPARATOR)) {
			normalizedCode = code.substring(0, code.indexOf(CODE_SEPARATOR));
		}
		return normalizedCode;
	}

	@Data
	public static class FileSharedInfo {
		private Whitelist whitelist;
		private Blacklist blacklist;
		private UsersAccessInfo usersAccessInfo;
		private SharingType sharingType;
		private Set<GroupAction> groupActions;

		public FileSharedInfo() {
			this(new Whitelist(), new Blacklist(), new UsersAccessInfo(), SharingType.PUBLIC, new HashSet<>());
		}

		public FileSharedInfo(Whitelist whitelist, Blacklist blacklist, UsersAccessInfo usersAccessInfo, SharingType sharingType, Set<GroupAction> groupActions) {
			this.whitelist = whitelist;
			this.blacklist = blacklist;
			this.usersAccessInfo = usersAccessInfo;
			this.sharingType = sharingType != null ? sharingType : SharingType.PUBLIC;
			this.groupActions = groupActions != null ? groupActions : new HashSet<>();
		}

		@Getter
		public enum SharingType {
			PUBLIC("public"),                  // Anyone can access
			PUBLIC_BY_LOGIN("publicByLogin"),  // Public, but login required, owner doesn't see accessed users
			GROUP_BY_LOGIN("groupByLogin");    // Group access with login, owner sees accessed users

			private final String type;

			SharingType(String type) {
				this.type = type;
			}

			public static SharingType fromType(String type) {
				for (SharingType sharingType : values()) {
					if (sharingType.getType().equalsIgnoreCase(type)) {
						return sharingType;
					}
				}
				return null;
			}
		}

		@Getter
		public enum GroupAction {
			EDIT_WHITELIST("editWhitelist"),
			EDIT_BLACKLIST("editBlacklist"),
			VIEW_USERS_ACCESS("viewUsersAccess");

			private final String action;

			GroupAction(String action) {
				this.action = action;
			}

			public static GroupAction fromAction(String action) {
				for (GroupAction groupAction : values()) {
					if (groupAction.getAction().equalsIgnoreCase(action)) {
						return groupAction;
					}
				}
				return null;
			}
		}

		public enum PermissionType {
			READ,
			WRITE,
		}

		@Data
		public static class Whitelist {
			private Map<Integer, Permission> permissions;

			public Whitelist() {
				this(new HashMap<>());
			}

			public Whitelist(Map<Integer, Permission> permissions) {
				this.permissions = permissions != null ? permissions : new HashMap<>();
			}

		}

		@Data
		public static class Permission {
			private PermissionType permissionType;

			public Permission(PermissionType permissionType) {
				this.permissionType = permissionType;
			}

		}

		@Data
		public static class Blacklist {
			private Set<Integer> users;

			public Blacklist() {
				this(new HashSet<>());
			}

			public Blacklist(Set<Integer> users) {
				this.users = users != null ? users : new HashSet<>();
			}

		}

		@Data
		public static class UsersAccessInfo {
			private Map<Integer, Long> users;

			public UsersAccessInfo() {
				this(new HashMap<>());
			}

			public UsersAccessInfo(Map<Integer, Long> users) {
				this.users = users != null ? users : new HashMap<>();
			}

		}

	}
}
