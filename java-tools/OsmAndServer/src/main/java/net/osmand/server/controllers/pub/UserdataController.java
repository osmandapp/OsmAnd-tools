package net.osmand.server.controllers.pub;

import static org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE;

import java.io.*;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import net.osmand.server.api.services.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserDevicesRepository.PremiumUserDevice;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFile;
import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.PremiumUsersRepository;
import net.osmand.server.api.repo.PremiumUsersRepository.PremiumUser;
import net.osmand.util.Algorithms;

@RestController
@RequestMapping("/userdata")
public class UserdataController {
	private static final int ERROR_CODE_PREMIUM_USERS = 100;
	private static final int ERROR_CODE_EMAIL_IS_INVALID = 1 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_NO_VALID_SUBSCRIPTION = 2 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_SUBSCRIPTION_WAS_USED_FOR_ANOTHER_ACCOUNT = 9 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_USER_IS_ALREADY_REGISTERED = 11 + ERROR_CODE_PREMIUM_USERS;

	protected static final Log LOG = LogFactory.getLog(UserdataController.class);


	Gson gson = new Gson();

	@Autowired
	PasswordEncoder encoder;

	@Autowired
	protected PremiumUsersRepository usersRepository;

	@Autowired
	protected PremiumUserFilesRepository filesRepository;

	@Autowired
	protected PremiumUserDevicesRepository devicesRepository;

	@Autowired
	protected StorageService storageService;

	@Autowired
	protected DownloadIndexesService downloadService;
	
	@Autowired
	protected UserSubscriptionService userSubService;

	@Autowired
	EmailSenderService emailSender;
	
	@Autowired
	UserdataService userdataService;

	
	private PremiumUserDevice checkToken(int deviceId, String accessToken) {
		PremiumUserDevice d = devicesRepository.findById(deviceId);
		if (d != null && Algorithms.stringsEqual(d.accesstoken, accessToken)) {
			return d;
		}
		return null;
	}

	@GetMapping(value = "/user-validate-sub")
	@ResponseBody
	public ResponseEntity<String> check(@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken) throws IOException {
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValid();
		}
		PremiumUser pu = usersRepository.findById(dev.userid);
		if (pu == null) {
			return userdataService.error(ERROR_CODE_EMAIL_IS_INVALID, "email is not registered");
		}
		String errorMsg = userSubService.checkOrderIdPremium(pu.orderid);
		if (errorMsg != null) {
			return userdataService.error(ERROR_CODE_NO_VALID_SUBSCRIPTION, errorMsg);
		}
		return ResponseEntity.ok(gson.toJson(pu));
	}

    @GetMapping(value = "/user-validate-ios-sub")
    @ResponseBody
    public ResponseEntity<String> checkOrderid(@RequestParam(name = "orderid", required = true) String orderid) throws IOException {
        JsonObject json = userSubService.revalidateiOSSubscription(orderid);
        if (json.has("error")) {
            return userdataService.error(ERROR_CODE_NO_VALID_SUBSCRIPTION, json.get("error").getAsString());
        }
        return ResponseEntity.ok(json.toString());
    }

	@PostMapping(value = "/user-update-orderid")
	@ResponseBody
	public ResponseEntity<String> userUpdateOrderid(@RequestParam(name = "email", required = true) String email,
			@RequestParam(name = "deviceid", required = false) String deviceId,
			@RequestParam(name = "orderid", required = false) String orderid) throws IOException {
		PremiumUser pu = usersRepository.findByEmail(email);
		if (pu == null) {
			return userdataService.error(ERROR_CODE_EMAIL_IS_INVALID, "email is not registered");
		}
		String errorMsg = userSubService.checkOrderIdPremium(orderid);
		if (errorMsg != null) {
			return userdataService.error(ERROR_CODE_NO_VALID_SUBSCRIPTION, errorMsg);
		}
		PremiumUser otherUser = usersRepository.findByOrderid(orderid);
		if (otherUser != null && !Algorithms.objectEquals(pu.orderid, orderid)) {
			String hideEmail = userdataService.hideEmail(otherUser.email);
			return userdataService.error(ERROR_CODE_SUBSCRIPTION_WAS_USED_FOR_ANOTHER_ACCOUNT,
					"user was already signed up as " + hideEmail);
		}
		pu.orderid = orderid;
		usersRepository.saveAndFlush(pu);
		return userdataService.ok();
	}

	@PostMapping(value = "/user-register")
	@ResponseBody
	public ResponseEntity<String> userRegister(@RequestParam(name = "email", required = true) String email,
			@RequestParam(name = "deviceid", required = false) String deviceId,
			@RequestParam(name = "orderid", required = false) String orderid,
			@RequestParam(name = "login", required = false) boolean login) throws IOException {
		// allow to register only with small case
		email = email.toLowerCase().trim();
		PremiumUser pu = usersRepository.findByEmail(email);
		if (!email.contains("@")) {
			return userdataService.error(ERROR_CODE_EMAIL_IS_INVALID, "email is not valid to be registered");
		}
		if (pu != null) {
			if (!login) {
				return userdataService.error(ERROR_CODE_USER_IS_ALREADY_REGISTERED, "user was already registered with such email");
			}
			// don't check order id validity for login
		} else {
			String error = userSubService.checkOrderIdPremium(orderid);
			if (error != null) {
				return userdataService.error(ERROR_CODE_NO_VALID_SUBSCRIPTION, error);
			}
			PremiumUser otherUser = usersRepository.findByOrderid(orderid);
			if (otherUser != null) {
				String hideEmail = userdataService.hideEmail(otherUser.email);
				List<PremiumUserDevice> pud = devicesRepository.findByUserid(otherUser.id);
				// check that user already registered at least 1 device (avoid typos in email)
				if (pud != null && !pud.isEmpty()) {
					return userdataService.error(ERROR_CODE_SUBSCRIPTION_WAS_USED_FOR_ANOTHER_ACCOUNT,
							"user was already signed up as " + hideEmail);
				} else {
					otherUser.orderid = null;
					usersRepository.saveAndFlush(otherUser);
				}
			}
			pu = new PremiumUsersRepository.PremiumUser();
			pu.email = email;
			pu.regTime = new Date();
			pu.orderid = orderid;
		}
		// keep old order id
		pu.tokendevice = deviceId;
		pu.token = (new Random().nextInt(8999) + 1000) + "";
		pu.tokenTime = new Date();
		usersRepository.saveAndFlush(pu);
		emailSender.sendOsmAndCloudRegistrationEmail(pu.email, pu.token, true);
		return userdataService.ok();
	}

	@PostMapping(value = "/device-register")
	@ResponseBody
	public ResponseEntity<String> deviceRegister(@RequestParam(name = "email", required = true) String email,
			@RequestParam(name = "token", required = true) String token,
			@RequestParam(name = "deviceid", required = false) String deviceId) throws IOException {
		String accessToken = UUID.randomUUID().toString();
		return userdataService.registerNewDevice(email, token, deviceId, accessToken);
	}

	@PostMapping(value = "/delete-file")
	@ResponseBody
	public ResponseEntity<String> delete(@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken,
			@RequestParam(name = "clienttime", required = false) Long clienttime) throws IOException {
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValid();
		}
		UserFile usf = new PremiumUserFilesRepository.UserFile();
		usf.name = name;
		usf.type = type;
		usf.updatetime = new Date();
		usf.userid = dev.userid;
		usf.deviceid = deviceId;
		usf.data = null;
		usf.filesize = -1l;
		usf.zipfilesize = -1l;
		if (clienttime != null) {
			usf.clienttime = new Date(clienttime.longValue());
		}
		filesRepository.saveAndFlush(usf);
		return userdataService.ok();
	}

	@PostMapping(value = "/delete-file-version")
	@ResponseBody
	public ResponseEntity<String> deleteFile(HttpServletResponse response, HttpServletRequest request,
			@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "updatetime", required = true) Long updatetime,
			@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken) throws IOException, SQLException {
		UserFile fl = null;
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValid();
		} else {
			if (updatetime != null) {
				fl = filesRepository.findTopByUseridAndNameAndTypeAndUpdatetime(dev.userid, name, type,
						new Date(updatetime));
			}
			if (fl == null) {
				return userdataService.error(userdataService.ERROR_CODE_FILE_NOT_AVAILABLE, "File is not available");
			}
			storageService.deleteFile(fl.storage,userdataService.userFolder(fl), userdataService.storageFileName(fl));
			filesRepository.delete(fl);
			return userdataService.ok();
		}
	}

	@PostMapping(value = "/upload-file", consumes = MULTIPART_FORM_DATA_VALUE)
	@ResponseBody
	public ResponseEntity<String> upload(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file,
			@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "deviceid", required = false) Integer deviceId,
			@RequestParam(name = "accessToken", required = false) String accessToken,
			@RequestParam(name = "clienttime", required = false) Long clienttime) throws IOException {
		// This could be slow series of checks (token, user, subscription, amount of space):
		// probably it's better to support multiple file upload without these checks
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		
		if (dev == null) {
			return userdataService.tokenNotValid();
		}
		PremiumUser pu = usersRepository.findById(dev.userid);
		ResponseEntity<String> validateError = userdataService.validateUser(pu);
		if (validateError != null) {
			return validateError;
		}
		
		ResponseEntity<String> uploadError = userdataService.uploadFile(file, dev, name, type, deviceId);
		if (uploadError != null) {
			return uploadError;
		}

		return userdataService.ok();
	}

	@GetMapping(value = "/check-file-on-server")
	@ResponseBody
	public ResponseEntity<String> checkFileOnServer(@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type) throws IOException {
		if (userdataService.checkThatObfFileisOnServer(name, type)) {
			return ResponseEntity.ok(gson.toJson(Collections.singletonMap("status", "present")));
		}
		return ResponseEntity.ok(gson.toJson(Collections.singletonMap("status", "not-present")));
	}

	@PostMapping(value = "/remap-filenames")
	@ResponseBody
	public ResponseEntity<String> remapFilenames(@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken) throws IOException, SQLException {
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValid();
		}
		// remap needs to happen to all users & temporarily service should find files by both names (download)
		Iterable<UserFile> lst = filesRepository.findAllByUserid(dev.userid);
		for (UserFile fl : lst) {
			if (fl != null && fl.filesize > 0) {
				storageService.remapFileNames(fl.storage, userdataService.userFolder(fl), userdataService.oldStorageFileName(fl), userdataService.storageFileName(fl));
			}
		}
		return userdataService.ok();
	}

	@PostMapping(value = "/backup-storage")
	@ResponseBody
	public ResponseEntity<String> migrateData(@RequestParam(name = "storageid", required = true) String storageId,
			@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken) throws IOException, SQLException {
		if (!storageService.hasStorageProviderById(storageId)) {
			return userdataService.error(400, "Storage id is not configured");
		}
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValid();
		}
		Iterable<UserFile> lst = filesRepository.findAllByUserid(dev.userid);
		for (UserFile fl : lst) {
			if (fl != null && fl.filesize > 0) {
				String newStorage = storageService.backupData(storageId, userdataService.userFolder(fl), userdataService.storageFileName(fl),
						fl.storage, fl.data);
				if (newStorage != null) {
					fl.storage = newStorage;
					filesRepository.save(fl);
				}
			}
		}
		return userdataService.ok();
	}

	@GetMapping(value = "/download-file")
	@ResponseBody
	public void getFile(HttpServletResponse response, HttpServletRequest request,
			@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "updatetime", required = false) Long updatetime,
			@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken) throws IOException, SQLException {
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		userdataService.getFile(response, request, name, type, updatetime, dev);
	}

	@GetMapping(value = "/list-files")
	@ResponseBody
	public ResponseEntity<String> listFiles(@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken,
			@RequestParam(name = "name", required = false) String name,
			@RequestParam(name = "type", required = false) String type,
			@RequestParam(name = "allVersions", required = false, defaultValue = "false") boolean allVersions)
			throws IOException, SQLException {
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValid();
		}
		UserFilesResults res = userdataService.generateFiles(dev.userid, name, type, allVersions, false);
		return ResponseEntity.ok(gson.toJson(res));
	}

	public static class UserFilesResults {
		public int totalZipSize;
		public int totalFileSize;
		public int totalFiles;
		public int totalFileVersions;
		public List<UserFileNoData> allFiles;
		public List<UserFileNoData> uniqueFiles;
		public int userid;
		public long maximumAccountSize;

	}
}
