package net.osmand.server.api.services.shareGpx;

import com.google.gson.Gson;
import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.api.repo.PremiumUsersRepository;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import okio.Buffer;
import okio.Source;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static net.osmand.server.api.services.shareGpx.FileSharedInfo.PermissionType.READ;

@Service
public class ShareGpxService {

	@Autowired
	protected PremiumUserFilesRepository filesRepository;

	@Autowired
	protected PremiumUsersRepository usersRepository;

	private static final String BLACK_LIST = "blacklist";
	private static final String WHITE_LIST = "whitelist";
	private static final String ACCESSED_USERS = "accessedUsers";

	Gson gson = new Gson();

	@Transactional
	public String generateSharedCode(PremiumUserFilesRepository.UserFile userFile) {
		String uniqueToken = UUID.randomUUID().toString();
		userFile.sharedCode = uniqueToken;
		filesRepository.saveAndFlush(userFile);

		return uniqueToken;
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

	public boolean saveAccessedUser(PremiumUserDevicesRepository.PremiumUserDevice dev, PremiumUserFilesRepository.UserFile userFile) {
		FileSharedInfo fileSharedInfo = gson.fromJson(userFile.sharedInfo, FileSharedInfo.class);
		if (fileSharedInfo == null) {
			fileSharedInfo = new FileSharedInfo();
		}
		if (!fileSharedInfo.getBlacklist().getUsers().contains(dev.userid)) {
			if (fileSharedInfo.getAccessedUsers().getUsers().add(dev.userid)) {
				userFile.sharedInfo.add(ACCESSED_USERS, gson.toJsonTree(fileSharedInfo.getAccessedUsers()));
				filesRepository.saveAndFlush(userFile);
			}
			return true;
		}
		return false;
	}

	public List<String> getAccessedUsers(PremiumUserFilesRepository.UserFile userFile) {
		List<String> accessedUsers = new ArrayList<>();
		FileSharedInfo fileSharedInfo = gson.fromJson(userFile.sharedInfo, FileSharedInfo.class);
		if (fileSharedInfo != null && fileSharedInfo.getAccessedUsers() != null) {
			for (Integer userId : fileSharedInfo.getAccessedUsers().getUsers()) {
				PremiumUsersRepository.PremiumUser pu = usersRepository.findById(userId);
				if (pu != null) {
					accessedUsers.add(pu.email);
				}
			}
		}
		return accessedUsers;
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
				m.put(pu.id, new FileSharedInfo.Permission(READ));
			}
		}, HashMap::putAll));
		if (wlist.getPermissions().isEmpty()) {
			return false;
		}
		userFile.sharedInfo.add(WHITE_LIST, gson.toJsonTree(wlist));
		filesRepository.saveAndFlush(userFile);
		return true;
	}

	public List<String> getBlackList(PremiumUserFilesRepository.UserFile userFile) {
		List<String> blackList = new ArrayList<>();
		FileSharedInfo fileSharedInfo = gson.fromJson(userFile.sharedInfo, FileSharedInfo.class);
		if (fileSharedInfo != null && fileSharedInfo.getBlacklist() != null) {
			for (Integer userId : fileSharedInfo.getBlacklist().getUsers()) {
				PremiumUsersRepository.PremiumUser pu = usersRepository.findById(userId);
				if (pu != null) {
					blackList.add(pu.email);
				}
			}
		}
		return blackList;
	}
}
