package net.osmand.server.controllers.pub;

import static net.osmand.server.api.repo.DeviceInAppPurchasesRepository.*;
import static net.osmand.server.api.repo.DeviceSubscriptionsRepository.*;
import static net.osmand.server.api.repo.SupportersRepository.*;
import static org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import net.osmand.server.api.repo.*;
import net.osmand.server.api.services.*;
import net.osmand.server.api.services.DownloadIndexesService.ServerCommonFile;

import net.osmand.server.controllers.user.MapApiController;
import net.osmand.server.utils.exception.OsmAndPublicApiException;
import net.osmand.server.ws.UserTranslationsService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;

import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.util.Algorithms;

@RestController
@RequestMapping("/userdata")
public class UserdataController {
	private static final int ERROR_CODE_PRO_USERS = 100;
	private static final int ERROR_CODE_EMAIL_IS_INVALID = 1 + ERROR_CODE_PRO_USERS;
	private static final int ERROR_CODE_NO_VALID_SUBSCRIPTION = 2 + ERROR_CODE_PRO_USERS;
	private static final int ERROR_CODE_SUBSCRIPTION_WAS_USED_FOR_ANOTHER_ACCOUNT = 9 + ERROR_CODE_PRO_USERS;
	private static final int ERROR_CODE_USER_IS_ALREADY_REGISTERED = 11 + ERROR_CODE_PRO_USERS;
	private static boolean DISCARD_ANOTHER_USER_PAYMENT_IF_NEW_REGISTERED = true;

	protected static final Log LOG = LogFactory.getLog(UserdataController.class);

	// This is a permanent token for users who can't receive email but validated identity differently
	public static final int SPECIAL_PERMANENT_TOKEN = 8;


	Gson gson = new Gson();

	@Autowired
	PasswordEncoder encoder;

	@Autowired
	protected CloudUsersRepository usersRepository;

	@Autowired
	protected CloudUserFilesRepository filesRepository;

	@Autowired
	protected CloudUserDevicesRepository devicesRepository;

	@Autowired
	protected StorageService storageService;

	@Autowired
	protected DownloadIndexesService downloadService;

	@Autowired
	protected UserSubscriptionService userSubService;
	
	@Autowired
	protected UserTranslationsService websocketController;

	@Autowired
	EmailSenderService emailSender;

	@Autowired
	UserdataService userdataService;

    @Autowired
    DeviceInAppPurchasesRepository iapsRepository;

    @Autowired
    DeviceSubscriptionsRepository subscriptionsRepository;

    @Autowired
    CloudUsersRepository premiumUsersRepository;

    @Autowired
    SupportersRepository supportersRepository;

	public static class TokenPost {
		public String token;
	}

	public static class EmailSenderInfo {
		public String action;
		public String lang;
	}

	private CloudUserDevice checkToken(int deviceId, String accessToken) {
		CloudUserDevice d = devicesRepository.findById(deviceId);
		if (d != null && Algorithms.stringsEqual(d.accesstoken, accessToken)) {
			return d;
		}
		return null;
	}

	public ResponseEntity<String> invalidateUser(@RequestParam(required = true) int userId) throws IOException {
		UserFilesResults res = userdataService.generateFiles(userId, null, false, false, Collections.emptySet());
		Iterator<UserFileNoData> it = res.uniqueFiles.iterator();
		StringBuilder sb = new StringBuilder();
		while (it.hasNext()) {
			UserFileNoData ufnd = it.next();
			ServerCommonFile scf = userdataService.checkThatObfFileisOnServer(ufnd.name, ufnd.type);
			if (scf != null && ufnd.zipSize > 0) {
				sb.append(" - ").append(ufnd.name + ". ");
				boolean upd = false;
				if (ufnd.zipSize > 1000) {
					ufnd.zipSize = 40;
					sb.append(" Update zip size.");
					upd = true;
				}
				if (ufnd.filesize < scf.di.getContentSize() / 3) {
					ufnd.filesize = scf.di.getContentSize();
					sb.append(" Update file size to " + scf.di.getContentSize());
					upd = true;
				}
				if (upd) {
					userdataService.updateFileSize(ufnd);
					sb.append(" Saved.");
				}
				sb.append("<br>\n");
			}
		}

		return ResponseEntity.ok(sb.toString());
	}

	@GetMapping(value = "/user-validate-sub")
	public ResponseEntity<String> check(@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken,
			HttpServletRequest request) throws IOException {
		CloudUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValidError();
		}
		CloudUser pu = usersRepository.findById(dev.userid);
		if (pu == null) {
			logErrorWithThrow(request, ERROR_CODE_EMAIL_IS_INVALID, "email is not registered");
		}
		String errorMsg = userSubService.verifyAndRefreshProOrderId(pu);
		if (errorMsg != null) {
			logErrorWithThrow(request, ERROR_CODE_NO_VALID_SUBSCRIPTION, errorMsg);
		}
		return ResponseEntity.ok(gson.toJson(pu));
	}
	
	@GetMapping(value = "/translation/msg")
	public ResponseEntity<String> sendMessage(@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken,
			@RequestParam(name = "translationId", required = true) String translationId,
			HttpServletRequest request) throws IOException {
		CloudUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValidError();
		}
		CloudUser pu = usersRepository.findById(dev.userid);
		if (pu == null) {
			logErrorWithThrow(request, ERROR_CODE_EMAIL_IS_INVALID, "email is not registered");
		}
		return websocketController.sendMessage(translationId, dev, pu, request); 
	}
	
	@GetMapping(value = "/translation/create")
	public ResponseEntity<String> createTranslation(@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken,
			@RequestParam(name = "translationId", required = true) String translationId,
			HttpServletRequest request) throws IOException {
		CloudUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValidError();
		}
		CloudUser pu = usersRepository.findById(dev.userid);
		if (pu == null) {
			logErrorWithThrow(request, ERROR_CODE_EMAIL_IS_INVALID, "email is not registered");
		}
		return websocketController.sendMessage(translationId, dev, pu, request); 
	}

	@PostMapping(value = "/user-update-orderid")
	public ResponseEntity<String> userUpdateOrderid(@RequestParam String email,
	                                                @RequestParam(name = "deviceid", required = false) String deviceId,
	                                                @RequestParam(required = false) String orderid,
	                                                HttpServletRequest request) {
		email = email.toLowerCase().trim();
		CloudUser pu = usersRepository.findByEmailIgnoreCase(email);
		if (pu == null) {
			logErrorWithThrow(request, ERROR_CODE_EMAIL_IS_INVALID, "email is not registered");
		}
		pu.orderid = orderid;

		usersRepository.saveAndFlush(pu);

		if (orderid != null) {
			discardPreviousAccountOrderId(pu.id, orderid, email, request);
		}

		userSubService.verifyAndRefreshProOrderId(pu);

		return userdataService.ok();
	}

    @PostMapping(value = "/user-register")
    @Transactional // Make linking atomic with registration
    public ResponseEntity<String> userRegister(@RequestParam String email,
                                               @RequestParam(name = "deviceid", required = false) String deviceId, // PremiumUserDevice deviceId (e.g., "web")
                                               @RequestParam(required = false) String orderid, // Subscription orderId
                                               @RequestParam(required = false) Boolean login,
                                               @RequestParam(required = false) String lang,
                                               @RequestParam(required = false) String userId,
                                               @RequestParam(required = false) String userToken,
                                               HttpServletRequest request) {
		// allow to register only with small case
		email = email.toLowerCase().trim();
		CloudUser pu = usersRepository.findByEmailIgnoreCase(email);
		if (!email.contains("@")) {
			logErrorWithThrow(request, ERROR_CODE_EMAIL_IS_INVALID, "email is not valid to be registered");
		}
		if (pu != null) {
			if (!Boolean.TRUE.equals(login)) {
				logErrorWithThrow(request, ERROR_CODE_USER_IS_ALREADY_REGISTERED, "user was already registered with such email");
			}
			// don't check order id validity for login
			// keep old order id
		} else {
			pu = new CloudUsersRepository.CloudUser();
			pu.email = email;
			pu.regTime = new Date();
			pu.orderid = orderid;
		}
		if (orderid != null && pu.orderid == null) {
			pu.orderid = orderid;
		}
		pu.tokendevice = deviceId;
		pu.tokenTime = new Date();
		if (pu.token == null || pu.token.length() < SPECIAL_PERMANENT_TOKEN) {
			// see comment on constant
			pu.token = (new Random().nextInt(8999) + 1000) + "";
			// TODO iOS: add lang in OARegisterUserCommand.m before sendRequestWithUrl params[@"lang"] = ...
			emailSender.sendOsmAndCloudRegistrationEmail(pu.email, pu.token, lang, true);
		}
		CloudUser saved = usersRepository.saveAndFlush(pu);
	    if (orderid != null) {
		    discardPreviousAccountOrderId(saved.id, orderid, email, request);
	    }

        // --- Attempt to Link Supporter IAPs ---
        if (!Algorithms.isEmpty(userId) && !Algorithms.isEmpty(userToken)) {
            Supporter supporter = null;
            try {
                Optional<Supporter> supOpt = supportersRepository.findById(Long.parseLong(userId));
                supporter = supOpt.orElse(null);
                if (supporter != null && !userToken.equals(supporter.token)) {
                    LOG.warn("Supporter token mismatch during cloud registration for supporterId: " + userId);
                    supporter = null;
                }
            } catch (NumberFormatException e) {
                LOG.warn("Supporter ID is in wrong format: " + userId);
            }
            if (supporter != null) {
                // Supporter verified, find their IAPs
                List<SupporterDeviceInAppPurchase> iapsToLink = iapsRepository.findBySupporterId(supporter.userId);
                int linkedCount = 0;
                for (SupporterDeviceInAppPurchase iap : iapsToLink) {
                    // Update the userId to the PremiumUser ID
                    if (iap.userId == null) { // Check if update is needed
                        iap.userId = pu.id;
                        iapsRepository.save(iap); // Save the updated IAP record
                        linkedCount++;
                    }
                }
                if (linkedCount > 0) {
                    LOG.info("Linked " + linkedCount + " IAPs from Supporter " + userId + " to PremiumUser " + pu.id + " during cloud registration.");
                }
                // Optionally link subscription orderId if needed
                List<SupporterDeviceSubscription> subsToLink = subscriptionsRepository.findAllBySupporterId(supporter.userId);
                linkedCount = 0;
                for (SupporterDeviceSubscription sub : subsToLink) {
                    // Update the userId to the PremiumUser ID
                    if (sub.userId == null) { // Check if update is needed
                        sub.userId = pu.id;
                        subscriptionsRepository.save(sub); // Save the updated subscription record
                        linkedCount++;
                    }
                }
                if (linkedCount > 0) {
                    LOG.info("Linked " + linkedCount + " Subscriptions from Supporter " + userId + " to PremiumUser " + pu.id + " during cloud registration.");
                }
            } else {
                LOG.warn("Supporter not found during cloud registration for supporterId: " + userId);
            }
        } else {
            LOG.info("No supporter context provided during cloud registration for email: " + email);
        }
        // --- End Linking Logic ---

	    if (pu != null) {
		    userSubService.verifyAndRefreshProOrderId(pu);
	    }

		return userdataService.ok();
	}

	protected void discardPreviousAccountOrderId(int newUserId, String orderid, String currentEmail, HttpServletRequest request) {
		CloudUser previousUser =
				usersRepository.findFirstByOrderidAndEmailNotIgnoreCaseOrderByIdAsc(orderid, currentEmail);
		if (previousUser != null) {
			if (DISCARD_ANOTHER_USER_PAYMENT_IF_NEW_REGISTERED) {
				LOG.info("Discarding orderId " + orderid + " from previous user " + previousUser.id + " because it was used to register new user " + newUserId);
				userSubService.relinkPurchasesToNewAccount(previousUser, newUserId);
			} else {
				logErrorWithThrow(request, ERROR_CODE_SUBSCRIPTION_WAS_USED_FOR_ANOTHER_ACCOUNT,
						"user has already signed up as " + userdataService.hideEmail(previousUser.email));
			}
		}
	}

	@PostMapping(value = "/device-register")
	public ResponseEntity<String> deviceRegister(@RequestParam(name = "email", required = true) String email,
												 @RequestParam(name = "token", required = true) String token,
												 @RequestParam(name = "deviceid", required = false) String deviceId,
												 @RequestParam(name = "brand", required = false) String brand,
												 @RequestParam(name = "model", required = false) String model,
												 @RequestParam(name = "lang", required = false) String lang
	) throws IOException {
		email = email.toLowerCase().trim();
		String accessToken = UUID.randomUUID().toString();
		return userdataService.registerNewDevice(email, token, deviceId, accessToken, lang, brand, model);
	}

	@PostMapping(value = "/delete-file")
	public ResponseEntity<String> delete(@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken,
			@RequestParam(name = "clienttime", required = false) Long clienttime) throws IOException {
		CloudUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValidError();
		}
		userdataService.deleteFile(name, type, deviceId, clienttime, dev);
		return userdataService.ok();
	}

	@PostMapping(value = "/delete-file-version")
	public ResponseEntity<String> deleteFile(HttpServletResponse response, HttpServletRequest request,
	                                         @RequestParam(name = "name", required = true) String name,
	                                         @RequestParam(name = "type", required = true) String type,
	                                         @RequestParam(name = "updatetime", required = true) Long updatetime,
	                                         @RequestParam(name = "deviceid", required = true) int deviceId,
	                                         @RequestParam(name = "accessToken", required = true) String accessToken) {
		CloudUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValidError();
		} else {
			return userdataService.deleteFileVersion(updatetime, dev.userid, name, type, null);
		}
	}

	@PostMapping(value = "/upload-file", consumes = MULTIPART_FORM_DATA_VALUE)
	public ResponseEntity<String> upload(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file,
			@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = false) String accessToken,
			@RequestParam(name = "clienttime", required = false) Long clienttime) throws IOException {
		// This could be slow series of checks (token, user, subscription, amount of space):
		// probably it's better to support multiple file upload without these checks
		CloudUserDevice dev = checkToken(deviceId, accessToken);

		if (dev == null || name.contains("/../")) {
			return userdataService.tokenNotValidError();
		}
		return userdataService.uploadMultipartFile(file, dev, name, type, clienttime);
	}

	@GetMapping(value = "/check-file-on-server")
	public ResponseEntity<String> checkFileOnServer(@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type) throws IOException {
		if (userdataService.checkThatObfFileisOnServer(name, type) != null) {
			return ResponseEntity.ok(gson.toJson(Collections.singletonMap("status", "present")));
		}
		return ResponseEntity.ok(gson.toJson(Collections.singletonMap("status", "not-present")));
	}

	@PostMapping(value = "/remap-filenames")
	public ResponseEntity<String> remapFilenames(@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken) throws IOException, SQLException {
		CloudUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValidError();
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
	public ResponseEntity<String> migrateData(@RequestParam(name = "storageid", required = true) String storageId,
			@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken) throws IOException, SQLException {
		if (!storageService.hasStorageProviderById(storageId)) {
			throw new OsmAndPublicApiException(400, "Storage id is not configured");
		}
		CloudUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValidError();
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
	public void getFile(HttpServletResponse response, HttpServletRequest request,
	                    @RequestParam String name,
	                    @RequestParam String type,
	                    @RequestParam(required = false) Long updatetime,
	                    @RequestParam(name = "deviceid") int deviceId,
	                    @RequestParam String accessToken) throws IOException {
		CloudUserDevice dev = checkToken(deviceId, accessToken);
		if (dev != null) {
			CloudUserFilesRepository.UserFile userFile = userdataService.getUserFile(name, type, updatetime, dev);
			if (userFile != null) {
				userdataService.getFile(userFile, response, request, name, type, dev, false);
			}
		}
	}

	@GetMapping(value = "/list-files")
	public ResponseEntity<String> listFiles(@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken,
			@RequestParam(name = "name", required = false) String name,
			@RequestParam(name = "type", required = false) String type,
			@RequestParam(name = "allVersions", required = false, defaultValue = "false") boolean allVersions)
			throws IOException, SQLException {
		CloudUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValidError();
		}
		Set<String> types = userdataService.parseFileTypes(type);
		UserFilesResults res = userdataService.generateFiles(dev.userid, name, allVersions, false, types);
		return ResponseEntity.ok(gson.toJson(res));
	}

	// TokenPost for backward compatibility
	@PostMapping(path = {"/delete-account"})
	public ResponseEntity<String> deleteAccount(@RequestBody TokenPost token,
	                                            @RequestParam(name = "deviceid") int deviceId,
	                                            @RequestParam String accessToken,
			HttpServletRequest request) throws ServletException {
		CloudUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValidError();
		}
		return userdataService.deleteAccount(token.token, dev, request);
	}

	@PostMapping(path = {"/send-code"})
	public ResponseEntity<String> sendCode(@RequestBody EmailSenderInfo data,
	                                       @RequestParam(name = "deviceid") int deviceId,
			@RequestParam String accessToken) {
		CloudUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return userdataService.tokenNotValidError();
		}
		CloudUsersRepository.CloudUser pu = usersRepository.findById(dev.userid);
		if (pu == null) {
			return ResponseEntity.badRequest().body("User not found");
		}
		return userdataService.sendCode(data.action, data.lang, pu);
	}

	public static class UserFilesResults {
		public long totalZipSize;
		public long totalFileSize;
		public int totalFiles;
		public int totalFileVersions;
		public List<UserFileNoData> allFiles;
		public List<UserFileNoData> uniqueFiles;
		public int userid;
		public long maximumAccountSize;

	}

	private void logErrorWithThrow(HttpServletRequest request, int code, String msg) throws OsmAndPublicApiException {
		Map<String, String[]> params = request.getParameterMap();
		String url = request.getRequestURI();
		String ipAddress = request.getHeader("X-FORWARDED-FOR") == null ? request.getRemoteAddr() : request.getHeader("X-FORWARDED-FOR");
		String m = "";
		for (Map.Entry<String, String[]> e : params.entrySet()) {
			String v = e.getValue().length > 0 ? e.getValue()[0] : "EMPTY";
			m += ", " + e.getKey() + ":" + v;
		}
		LOG.error("URL:" + url + ", ip:" + ipAddress +  ", code:" + code + ", message:" + msg + m);
		throw new OsmAndPublicApiException(code, msg);
	}

	@PostMapping(path = {"/auth/confirm-code"})
	public ResponseEntity<String> confirmCode(@RequestBody MapApiController.UserPasswordPost credentials) {
		if (emailSender.isEmail(credentials.username)) {
			String username = credentials.username;
			String token = credentials.token;
			return userdataService.confirmCode(username, token);
		}
		return ResponseEntity.badRequest().body("Please enter valid email");
	}
}
