package net.osmand.server.api.services;

import net.osmand.server.api.repo.*;
import net.osmand.server.controllers.user.ShareFileController;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import okio.Buffer;
import okio.Source;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

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

	public enum PermissionType {
		READ,
		PENDING,
		BLOCKED
	}

	public static final String PRIVATE_SHARE_TYPE = "private";
	public static final String PUBLIC_SHARE_TYPE = "public";

	@Transactional
	public UUID generateSharedCode(PremiumUserFilesRepository.UserFile userFile, boolean publicAccess) {
		UUID uniqueCode = generateUniqueCode();
		// Update existing file with new code
		ShareFileRepository.ShareFile existingFile = getFileByOwnerAndFilepath(userFile.userid, userFile.name);
		if (existingFile != null) {
			existingFile.setUuid(uniqueCode);
			shareFileRepository.saveAndFlush(existingFile);
			return uniqueCode;
		}
		createShareFile(userFile, publicAccess, uniqueCode);

		return uniqueCode;
	}

	private UUID generateUniqueCode() {
		UUID uniqueCode;
		do {
			uniqueCode = UUID.randomUUID();
		} while (shareFileRepository.findByUuid(uniqueCode) != null);
		return uniqueCode;
	}

	@Transactional
	public ShareFileRepository.ShareFile createShareFile(PremiumUserFilesRepository.UserFile userFile, boolean publicAccess, UUID uniqueCode) {
		ShareFileRepository.ShareFile existingFile = getFileByOwnerAndFilepath(userFile.userid, userFile.name);
		if (existingFile != null) {
			throw new IllegalStateException("File already shared");
		}
		ShareFileRepository.ShareFile shareFile = new ShareFileRepository.ShareFile();

		String name = userFile.name.substring(userFile.name.lastIndexOf("/") + 1);

		shareFile.setUuid(uniqueCode);
		shareFile.setName(name);
		shareFile.setFilepath(userFile.name);
		shareFile.setType(userFile.type);
		shareFile.setOwnerid(userFile.userid);
		shareFile.setPublicAccess(publicAccess);

		shareFileRepository.saveAndFlush(shareFile);
		return shareFile;
	}

	public ShareFileRepository.ShareFile getFileByOwnerAndFilepath(int ownerid, String filepath) {
		return shareFileRepository.findByOwneridAndFilepath(ownerid, filepath);
	}

	public List<ShareFileRepository.ShareFile> getFilesByOwner(int ownerid) {
		return shareFileRepository.findByOwnerid(ownerid);
	}

	public ShareFileRepository.ShareFile getFileById(long id) {
		return shareFileRepository.findById(id).orElse(null);
	}

	@Transactional
	public boolean editAccessList(ShareFileRepository.ShareFile shareFile, Map<Integer, String> accessMap) {
		List<ShareFileRepository.ShareFilesAccess> accessList = shareFile.getAccessRecords();
		if (accessList == null) {
			accessList = new ArrayList<>();
		}
		accessList.forEach(access -> {
			String accessType = accessMap.get(access.getUser().id);
			if (accessType != null) {
				access.setAccess(accessType);
			}
		});
		shareFile.setAccessRecords(accessList);
		shareFileRepository.saveAndFlush(shareFile);
		return true;
	}

	@Transactional
	public boolean changeFileShareType(ShareFileRepository.ShareFile shareFile, String shareType) {
		if (shareType.equals(PRIVATE_SHARE_TYPE)) {
			shareFileRepository.delete(shareFile);
		} else {
			shareFile.setPublicAccess(shareType.equals(PUBLIC_SHARE_TYPE));
			if (shareFile.isPublicAccess() && shareFile.getUuid() == null) {
				UUID uuid = generateUniqueCode();
				shareFile.setUuid(uuid);
			}
			shareFileRepository.saveAndFlush(shareFile);
		}
		return true;
	}

	public ResponseEntity<String> checkAccessAndReturnError(ShareFileRepository.ShareFile file) {
		if (file.isPublicAccess()) {
			return null;
		} else {
			PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
			if (dev == null) {
				return userdataService.tokenNotValidResponse();
			}
			if (file.ownerid == dev.userid) {
				return null;
			}
			List<ShareFileRepository.ShareFilesAccess> accessList = file.getAccessRecords();
			for (ShareFileRepository.ShareFilesAccess access : accessList) {
				if (access.getUser().id == dev.userid) {
					if (access.getAccess().equals(ShareFileService.PermissionType.PENDING.name())) {
						return ResponseEntity.ok().body(ShareFileService.PermissionType.PENDING.name());
					} else if (access.getAccess().equals(ShareFileService.PermissionType.BLOCKED.name())) {
						return ResponseEntity.ok().body(ShareFileService.PermissionType.BLOCKED.name());
					}
					return null;
				}
			}
		}
		return ResponseEntity.ok().body("request");
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

	public ShareFileRepository.ShareFile getShareFileByUuid(String uuid) {
		if (uuid == null) {
			return null;
		}
		return shareFileRepository.findByUuid(UUID.fromString(uuid));
	}

	@Transactional
	public void requestAccess(ShareFileRepository.ShareFile shareFile, PremiumUserDevicesRepository.PremiumUserDevice dev, String nickname) {
		ShareFileRepository.ShareFilesAccess access = new ShareFileRepository.ShareFilesAccess();
		PremiumUsersRepository.PremiumUser user = userdataService.getUserById(dev.userid);
		if (user.nickname == null) {
			user.nickname = nickname;
			user = usersRepository.saveAndFlush(user);
		}
		access.setUser(user);
		access.setAccess(ShareFileService.PermissionType.PENDING.name());
		access.setRequestDate(new Date());

		shareFile.addAccessRecord(access);

		shareFileRepository.saveAndFlush(shareFile);
	}

	public PremiumUserFilesRepository.UserFile getUserFile(ShareFileRepository.ShareFile file) {
		return filesRepository.findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(file.ownerid, file.name, file.type);
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
	public boolean updateRequests(Map<Integer, String> accessMap) {
		for (Map.Entry<Integer, String> entry : accessMap.entrySet()) {
			Integer id = entry.getKey();
			String accessType = entry.getValue();
			if (accessType.equals("approved")) {
				accessType = ShareFileService.PermissionType.READ.name();
			}
			if (accessType.equals(ShareFileService.PermissionType.BLOCKED.name().toLowerCase())) {
				accessType = ShareFileService.PermissionType.BLOCKED.name();
			}
			ShareFileRepository.ShareFilesAccess access = shareFileRepository.findShareFilesAccessById(id);
			if (access != null) {
				access.setAccess(accessType);
				shareFileRepository.saveAndFlush(access);
			}
		}
		return true;
	}
}
