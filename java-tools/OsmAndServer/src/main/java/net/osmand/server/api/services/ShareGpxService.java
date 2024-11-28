package net.osmand.server.api.services;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.Data;
import lombok.Getter;
import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.api.repo.PremiumUsersRepository;
import net.osmand.server.controllers.user.ShareFileController;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import okio.Buffer;
import okio.Source;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import javax.transaction.Transactional;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static org.springframework.util.MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE;


@Service
public class ShareGpxService {

	@Autowired
	protected PremiumUserFilesRepository filesRepository;

	@Autowired
	protected PremiumUsersRepository usersRepository;

	@Autowired
	UserdataService userdataService;

	private static final String BLACK_LIST = "blacklist";
	private static final String WHITE_LIST = "whitelist";

	private static final String SEPARATOR = ",";

	Gson gson = new Gson();

	@Transactional
	public String generateSharedCode(PremiumUserFilesRepository.UserFile userFile, String type, @Nullable String groupActions) {
		String uniqueToken = UUID.randomUUID().toString();
		userFile.sharedCode = uniqueToken;
		userFile.sharedInfo = addSharedInfo(type, groupActions);
		filesRepository.saveAndFlush(userFile);

		return uniqueToken;
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

	public FileSharedInfo getSharedInfo(Long id) {
		JsonObject sharedInfoJson = filesRepository.findSharedInfoById(id);
		if (sharedInfoJson != null) {
			return gson.fromJson(sharedInfoJson, FileSharedInfo.class);
		}
		return null;
	}

	public FileSharedInfo getSharedInfo(String code) {
		JsonObject sharedInfoJson = filesRepository.findSharedInfoBySharedCode(code);
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

	public PremiumUserFilesRepository.UserFile getUserFileBySharedUrl(String token) {
		return filesRepository.findUserFileBySharedCode(token);
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
			return null;
		}
	}

	@Transactional
	public void saveAccessedUser(PremiumUserDevicesRepository.PremiumUserDevice dev, PremiumUserFilesRepository.UserFile userFile, FileSharedInfo info) {
		if (info == null) {
			info = new FileSharedInfo();
		}
		Map<Integer, Long> users = info.getAccessedUsers().getUsers();
		if (!users.containsKey(dev.userid)) {
			users.put(dev.userid, System.currentTimeMillis());
			info.getAccessedUsers().setUsers(users);
			userFile.sharedInfo = gson.toJsonTree(info).getAsJsonObject();
			filesRepository.saveAndFlush(userFile);
		}
	}

	@Transactional
	public boolean editBlacklist(PremiumUserFilesRepository.UserFile userFile, List<String> list) {
		FileSharedInfo fileSharedInfo = gson.fromJson(userFile.sharedInfo, FileSharedInfo.class);
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
		userFile.sharedInfo.add(BLACK_LIST, gson.toJsonTree(blist));
		filesRepository.saveAndFlush(userFile);
		return true;
	}

	@Transactional
	public boolean editWhitelist(PremiumUserFilesRepository.UserFile userFile, List<String> list) {
		FileSharedInfo fileSharedInfo = gson.fromJson(userFile.sharedInfo, FileSharedInfo.class);
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
		userFile.sharedInfo.add(WHITE_LIST, gson.toJsonTree(wlist));
		filesRepository.saveAndFlush(userFile);
		return true;
	}

	@Data
	public static class FileSharedInfo {
		private Whitelist whitelist;
		private Blacklist blacklist;
		private AccessedUsers accessedUsers;
		private SharingType sharingType;
		private Set<GroupAction> groupActions;

		public FileSharedInfo() {
			this(new Whitelist(), new Blacklist(), new AccessedUsers(), SharingType.PUBLIC, new HashSet<>());
		}

		public FileSharedInfo(Whitelist whitelist, Blacklist blacklist, AccessedUsers accessedUsers, SharingType sharingType, Set<GroupAction> groupActions) {
			this.whitelist = whitelist;
			this.blacklist = blacklist;
			this.accessedUsers = accessedUsers;
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
			SEE_ACCESSED_USERS("seeAccessedUsers");

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
		public static class AccessedUsers {
			private Map<Integer, Long> users;

			public AccessedUsers() {
				this(new HashMap<>());
			}

			public AccessedUsers(Map<Integer, Long> users) {
				this.users = users != null ? users : new HashMap<>();
			}

		}

	}
}
