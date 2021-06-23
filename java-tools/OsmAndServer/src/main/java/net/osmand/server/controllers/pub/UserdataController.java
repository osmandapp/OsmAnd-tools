package net.osmand.server.controllers.pub;

import static org.springframework.http.MediaType.APPLICATION_OCTET_STREAM;
import static org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.AndroidPublisher.Purchases.Subscriptions;
import com.google.api.services.androidpublisher.AndroidPublisherScopes;
import com.google.api.services.androidpublisher.model.SubscriptionPurchase;
import com.google.gson.Gson;

import net.osmand.live.subscriptions.UpdateSubscription;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscription;
import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserDevicesRepository.PremiumUserDevice;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFile;
import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.PremiumUsersRepository;
import net.osmand.server.api.repo.PremiumUsersRepository.PremiumUser;
import net.osmand.server.api.services.EmailSenderService;
import net.osmand.server.api.services.StorageService;
import net.osmand.util.Algorithms;

@RestController
@RequestMapping("/userdata")
public class UserdataController {

	private static final String USER_FOLDER_PREFIX = "user-";
	private static final String FILE_NAME_SUFFIX = ".gz";
	private static final int ERROR_CODE_PREMIUM_USERS = 100;
	private static final int BUFFER_SIZE = 1024 * 512;
	private static final long MB = 1024 * 1024;
	private static final long MAXIMUM_ACCOUNT_SIZE = 3000 * MB; // 3 (5 GB - std, 50 GB - ext, 1000 GB - premium)
	private static final int ERROR_CODE_EMAIL_IS_INVALID = 1 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_NO_VALID_SUBSCRIPTION = 2 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_USER_IS_NOT_REGISTERED = 3 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_TOKEN_IS_NOT_VALID_OR_EXPIRED = 4 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_PROVIDED_TOKEN_IS_NOT_VALID = 5 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_FILE_NOT_AVAILABLE = 6 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_GZIP_ONLY_SUPPORTED_UPLOAD = 7 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_SIZE_OF_SUPPORTED_BOX_IS_EXCEEDED = 8 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_SUBSCRIPTION_WAS_USED_FOR_ANOTHER_ACCOUNT = 9 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_SUBSCRIPTION_WAS_EXPIRED_OR_NOT_PRESENT = 10 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_USER_IS_ALREADY_REGISTERED = 11 + ERROR_CODE_PREMIUM_USERS;

	protected static final Log LOG = LogFactory.getLog(UserdataController.class);
	
	private static final String OSMAND_PRO_ANDROID_SUBSCRIPTION = UpdateSubscription.OSMAND_PRO_ANDROID_SUBSCRIPTION_PREFIX;
	private static final String OSMAND_PROMO_SUBSCRIPTION = "promo_";
	private static final String GOOGLE_PRODUCT_NAME = UpdateSubscription.GOOGLE_PRODUCT_NAME;
	private static final String GOOGLE_PACKAGE_NAME = UpdateSubscription.GOOGLE_PRODUCT_NAME;
	private static final String GOOGLE_PACKAGE_NAME_FREE = UpdateSubscription.GOOGLE_PRODUCT_NAME_FREE;
	
	Gson gson = new Gson();

	@Autowired
	protected DeviceSubscriptionsRepository subscriptionsRepo;

	@Autowired
	protected PremiumUsersRepository usersRepository;

	@Autowired
	protected PremiumUserFilesRepository filesRepository;

	@Autowired
	protected PremiumUserDevicesRepository devicesRepository;

	@Autowired
	protected StorageService storageService;
	
	@Autowired
	EmailSenderService emailSender;
	
	@Value("${google.androidPublisher.clientSecret}")
	protected String clientSecretFile; 
	
	private AndroidPublisher androidPublisher;

	// @PersistenceContext
	// protected EntityManager entityManager;

	protected ResponseEntity<String> error(int errorCode, String message) {
		Map<String, Object> mp = new TreeMap<String, Object>();
		mp.put("errorCode", errorCode);
		mp.put("message", message);
		return ResponseEntity.badRequest().body(gson.toJson(Collections.singletonMap("error", mp)));
	}

	private ResponseEntity<String> ok() {
		return ResponseEntity.ok(gson.toJson(Collections.singletonMap("status", "ok")));
	}
	
	private SupporterDeviceSubscription revalidateGoogleSubscription(SupporterDeviceSubscription s) {
		if (!Algorithms.isEmpty(clientSecretFile) ) {
			if (androidPublisher == null) {
				try {
					JacksonFactory jsonFactory = new com.google.api.client.json.jackson2.JacksonFactory();
					GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(jsonFactory,
							new InputStreamReader(new FileInputStream(clientSecretFile)));
					NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
					GoogleCredential credential = new GoogleCredential.Builder().setTransport(httpTransport)
							.setJsonFactory(jsonFactory).setServiceAccountId("user") // user constant?
							.setServiceAccountScopes(Collections.singleton(AndroidPublisherScopes.ANDROIDPUBLISHER))
							.setClientSecrets(clientSecrets).build();
					this.androidPublisher = new AndroidPublisher.Builder(httpTransport, jsonFactory, credential)
							.setApplicationName(GOOGLE_PRODUCT_NAME).build();
				} catch (Exception e) {
					LOG.error("Error configuring android publisher api: " + e.getMessage(), e);
				}
			}
			if (androidPublisher != null) {
				try {
					Subscriptions subs = androidPublisher.purchases().subscriptions();
					SubscriptionPurchase subscription;
					if (s.sku.contains("_free_")) {
						subscription = subs.get(GOOGLE_PACKAGE_NAME_FREE, s.sku, s.purchaseToken).execute();
					} else {
						subscription = subs.get(GOOGLE_PACKAGE_NAME, s.sku, s.purchaseToken).execute();
					}
					if (subscription != null) {
						if (s.expiretime == null || s.expiretime.getTime() < subscription.getExpiryTimeMillis()) {
							s.expiretime = new Date(subscription.getExpiryTimeMillis());
							s.checktime = new Date();
							s.valid = System.currentTimeMillis() < subscription.getExpiryTimeMillis();
							subscriptionsRepo.save(s);
						}
					}
				} catch (IOException e) {
					LOG.error(String.format("Error retrieving android publisher subscription %s - %s: %s", s.sku,
							s.orderId, e.getMessage()), e);
				}
			}
		}
		return s;
	}

	private String checkOrderIdPremium(String orderid) {
		if (Algorithms.isEmpty(orderid)) {
			return "no subscription provided";
		}
		String errorMsg = "no subscription present";
		List<SupporterDeviceSubscription> lst = subscriptionsRepo.findByOrderId(orderid);
		for (SupporterDeviceSubscription s : lst) {
			// s.sku could be checked for premium
			if (s.valid == null || s.valid.booleanValue()) {
				errorMsg = "no valid subscription present";
			} else if (!s.sku.startsWith(OSMAND_PRO_ANDROID_SUBSCRIPTION) && !s.sku.startsWith(OSMAND_PROMO_SUBSCRIPTION)) {
				errorMsg = "subscription is not eligible for OsmAnd Cloud";
			} else {
				if ((s.expiretime == null || s.checktime == null) && s.sku.startsWith(OSMAND_PRO_ANDROID_SUBSCRIPTION)) {
					s = revalidateGoogleSubscription(s);
				}
				if (s.expiretime != null && s.expiretime.getTime() > System.currentTimeMillis()) {
					return null;
				} else {
					errorMsg = "subscription is expired or not validated yet";
				}
			}
		}
		return errorMsg;
	}

	

	private PremiumUserDevice checkToken(int deviceId, String accessToken) {
		PremiumUserDevice d = devicesRepository.findById(deviceId);
		if (d != null && Algorithms.stringsEqual(d.accesstoken, accessToken)) {
			return d;
		}
		return null;
	}

	@PostMapping(value = "/user-update-orderid")
	@ResponseBody
	public ResponseEntity<String> userUpdateOrderid(@RequestParam(name = "email", required = true) String email,
			@RequestParam(name = "deviceid", required = false) String deviceId,
			@RequestParam(name = "orderid", required = false) String orderid) throws IOException {
		PremiumUser pu = usersRepository.findByEmail(email);
		if (pu == null) {
			return error(ERROR_CODE_EMAIL_IS_INVALID, "email is registered");
		}
		String errorMsg = checkOrderIdPremium(orderid);
		if (errorMsg != null) {
			return error(ERROR_CODE_NO_VALID_SUBSCRIPTION, errorMsg);
		}
		PremiumUser otherUser = usersRepository.findByOrderid(orderid);
		if (otherUser != null && !Algorithms.objectEquals(pu.orderid, orderid)) {
			String hideEmail = hideEmail(otherUser.email);
			return error(ERROR_CODE_SUBSCRIPTION_WAS_USED_FOR_ANOTHER_ACCOUNT,
					"user was already signed up as " + hideEmail);
		}
		pu.orderid = orderid;
		usersRepository.saveAndFlush(pu);
		return ok();
	}

	@PostMapping(value = "/user-register")
	@ResponseBody
	public ResponseEntity<String> userRegister(@RequestParam(name = "email", required = true) String email,
			@RequestParam(name = "deviceid", required = false) String deviceId,
			@RequestParam(name = "orderid", required = false) String orderid) throws IOException {
		PremiumUser pu = usersRepository.findByEmail(email);
		if (!email.contains("@")) {
			return error(ERROR_CODE_EMAIL_IS_INVALID, "email is not valid to be registered");
		}
		if (pu != null) {
			return error(ERROR_CODE_USER_IS_ALREADY_REGISTERED, "user was already registered with such email");
		} else {
			String error = checkOrderIdPremium(orderid);
			if (error != null) {
				return error(ERROR_CODE_NO_VALID_SUBSCRIPTION, error);
			}
			PremiumUser otherUser = usersRepository.findByOrderid(orderid);
			if (otherUser != null) {
				String hideEmail = hideEmail(otherUser.email);
				return error(ERROR_CODE_SUBSCRIPTION_WAS_USED_FOR_ANOTHER_ACCOUNT,
						"user was already signed up as " + hideEmail);
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
		emailSender.sendRegistrationEmail(pu.email, pu.token, true);
		return ok();
	}

	private String hideEmail(String email) {
		if (email == null) {
			return "***";
		}
		int at = email.indexOf("@");
		if (at == -1) {
			return email;
		}
		String name = email.substring(0, at);
		StringBuilder hdName = new StringBuilder();
		for (int i = 0; i < name.length(); i++) {
			char c = name.charAt(i);
			if (i > 0 && i < name.length() - 1) {
				c = '*';
			}
			hdName.append(c);
		}
		return hdName.toString() + email.substring(at);
	}

	@PostMapping(value = "/device-register")
	@ResponseBody
	public ResponseEntity<String> deviceRegister(@RequestParam(name = "email", required = true) String email,
			@RequestParam(name = "token", required = true) String token,
			@RequestParam(name = "deviceid", required = false) String deviceId) throws IOException {
		PremiumUser pu = usersRepository.findByEmail(email);
		if (pu == null) {
			return error(ERROR_CODE_USER_IS_NOT_REGISTERED, "user with that email is not registered");
		}
		if (pu.token == null || !pu.token.equals(token) || pu.tokenTime == null || System.currentTimeMillis()
				- pu.tokenTime.getTime() > TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS)) {
			return error(ERROR_CODE_TOKEN_IS_NOT_VALID_OR_EXPIRED, "token is not valid or expired (24h)");
		}
		pu.token = null;
		pu.tokenTime = null;
		PremiumUserDevice device = new PremiumUserDevice();
		device.userid = pu.id;
		device.deviceid = deviceId;
		device.udpatetime = new Date();
		device.accesstoken = UUID.randomUUID().toString();
		usersRepository.saveAndFlush(pu);
		devicesRepository.saveAndFlush(device);
		return ResponseEntity.ok(gson.toJson(device));
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
			return tokenNotValid();
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
		return ok();
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
			return tokenNotValid();
		} else {
			if (updatetime != null) {
				fl = filesRepository.findTopByUseridAndNameAndTypeAndUpdatetime(dev.userid, name, type,
						new Date(updatetime));
			}
			if (fl == null) {
				return error(ERROR_CODE_FILE_NOT_AVAILABLE, "File is not available");
			}
			storageService.deleteFile(fl.storage, userFolder(fl), storageFileName(fl));
			filesRepository.delete(fl);
			return ok();
		}
	}

	@PostMapping(value = "/upload-file", consumes = MULTIPART_FORM_DATA_VALUE)
	@ResponseBody
	public ResponseEntity<String> upload(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file,
			@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken,
			@RequestParam(name = "clienttime", required = false) Long clienttime) throws IOException {
		// This could be slow series of checks (token, user, subscription, amount of space):
		// probably it's better to support multiple file upload without these checks
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return tokenNotValid();
		}
		PremiumUser pu = usersRepository.findById(dev.userid);
		if (pu == null) {
			return error(ERROR_CODE_USER_IS_NOT_REGISTERED, "Unexpected error: user is not registered.");
		}
		String errorMsg = checkOrderIdPremium(pu.orderid);
		if (errorMsg != null) {
			return error(ERROR_CODE_SUBSCRIPTION_WAS_EXPIRED_OR_NOT_PRESENT,
					"Subscription is not valid any more: " + errorMsg);
		}
		UserFilesResults res = generateFiles(dev, null, null, false);
		if (res.totalZipSize > MAXIMUM_ACCOUNT_SIZE) {
			return error(ERROR_CODE_SIZE_OF_SUPPORTED_BOX_IS_EXCEEDED,
					"Maximum size of OsmAnd Cloud exceeded " + (MAXIMUM_ACCOUNT_SIZE / MB)
							+ " MB. Please contact support in order to investigate possible solutions.");
		}
		UserFile usf = new PremiumUserFilesRepository.UserFile();
		long cnt, sum;
		long zipsize = file.getSize();
		try {
			GZIPInputStream gzis = new GZIPInputStream(file.getInputStream());
			byte[] buf = new byte[1024];
			sum = 0;
			while ((cnt = gzis.read(buf)) >= 0) {
				sum += cnt;
			}
		} catch (IOException e) {
			return error(ERROR_CODE_GZIP_ONLY_SUPPORTED_UPLOAD, "File is submitted not in gzip format");
		}
		usf.name = name;
		usf.type = type;
		usf.updatetime = new Date();
		usf.userid = dev.userid;
		usf.deviceid = deviceId;
		usf.filesize = sum;
		usf.zipfilesize = zipsize;
		if (clienttime != null) {
			usf.clienttime = new Date(clienttime.longValue());
		}
		// Session session = entityManager.unwrap(Session.class);
		// Blob blob = session.getLobHelper().createBlob(file.getInputStream(), file.getSize());
		// usf.data = blob;
		usf.storage = storageService.save(userFolder(usf), storageFileName(usf), file);
		if (storageService.storeLocally()) {
			usf.data = file.getBytes();
		}
		filesRepository.saveAndFlush(usf);

		return ok();
	}

	private String oldStorageFileName(UserFile usf) {
		String fldName = usf.type;
		String name = usf.name;
		return fldName + "/" + usf.updatetime.getTime() + "-" + name + FILE_NAME_SUFFIX;
	}

	private String storageFileName(UserFile usf) {
		String fldName = usf.type;
		String name = usf.name;
		if (name.indexOf('/') != -1) {
			int nt = name.lastIndexOf('/');
			fldName += "/" + name.substring(0, nt);
			name = name.substring(nt + 1);
		}
		return fldName + "/" + usf.updatetime.getTime() + "-" + name + FILE_NAME_SUFFIX;
		// return usf.name + FILE_NAME_SUFFIX;
	}

	private String userFolder(UserFile usf) {
		return USER_FOLDER_PREFIX + usf.userid;
	}

	@PostMapping(value = "/remap-filenames")
	@ResponseBody
	public ResponseEntity<String> remapFilenames(@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken) throws IOException, SQLException {
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return tokenNotValid();
		}
		// remap needs to happen to all users & temporarily service should find files by both names (download)
		Iterable<UserFile> lst = filesRepository.findAllByUserid(dev.userid);
		for (UserFile fl : lst) {
			if (fl != null && fl.filesize > 0) {
				storageService.remapFileNames(fl.storage, userFolder(fl), oldStorageFileName(fl), storageFileName(fl));
			}
		}
		return ok();
	}

	@PostMapping(value = "/backup-storage")
	@ResponseBody
	public ResponseEntity<String> migrateData(@RequestParam(name = "storageid", required = true) String storageId,
			@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken) throws IOException, SQLException {
		if (!storageService.hasStorageProviderById(storageId)) {
			return error(400, "Storage id is not configured");
		}
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return tokenNotValid();
		}
		Iterable<UserFile> lst = filesRepository.findAllByUserid(dev.userid);
		for (UserFile fl : lst) {
			if (fl != null && fl.filesize > 0) {
				String newStorage = storageService.backupData(storageId, userFolder(fl), storageFileName(fl),
						fl.storage, fl.data);
				if (newStorage != null) {
					fl.storage = newStorage;
					filesRepository.save(fl);
				}
			}
		}
		return ok();
	}

	@GetMapping(value = "/download-file")
	@ResponseBody
	public void getFile(HttpServletResponse response, HttpServletRequest request,
			@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "updatetime", required = false) Long updatetime,
			@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken) throws IOException, SQLException {
		ResponseEntity<String> error = null;
		UserFile fl = null;
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		InputStream bin = null;
		try {
			if (dev == null) {
				error = tokenNotValid();
			} else {
				if (updatetime != null) {
					fl = filesRepository.findTopByUseridAndNameAndTypeAndUpdatetime(dev.userid, name, type,
							new Date(updatetime));
				} else {
					fl = filesRepository.findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(dev.userid, name, type);
				}
				if (fl == null) {
					error = error(ERROR_CODE_FILE_NOT_AVAILABLE, "File is not available");
				} else if (fl.data == null) {
					bin = storageService.getFileInputStream(fl.storage, userFolder(fl), storageFileName(fl));
					if (bin == null) {
						error = error(ERROR_CODE_FILE_NOT_AVAILABLE, "File is not available");
					}
				} else {
					bin = new ByteArrayInputStream(fl.data);
				}
			}

			if (error != null) {
				response.setStatus(error.getStatusCodeValue());
				response.getWriter().write(error.getBody());
				return;
			}
			response.setHeader("Content-Disposition", "attachment; filename=" + fl.name);
			// InputStream bin = fl.data.getBinaryStream();

			String acceptEncoding = request.getHeader("Accept-Encoding");
			if (acceptEncoding != null && acceptEncoding.contains("gzip")) {
				response.setHeader("Content-Encoding", "gzip");
			} else {
				bin = new GZIPInputStream(bin);
			}
			response.setContentType(APPLICATION_OCTET_STREAM.getType());
			byte[] buf = new byte[BUFFER_SIZE];
			int r;
			while ((r = bin.read(buf)) != -1) {
				response.getOutputStream().write(buf, 0, r);
			}
		} finally {
			if (bin != null) {
				bin.close();
			}
		}
	}

	private ResponseEntity<String> tokenNotValid() {
		return error(ERROR_CODE_PROVIDED_TOKEN_IS_NOT_VALID, "provided deviceid or token is not valid");
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
			return tokenNotValid();
		}
		UserFilesResults res = generateFiles(dev, name, type, allVersions);
		return ResponseEntity.ok(gson.toJson(res));
	}

	private UserFilesResults generateFiles(PremiumUserDevice dev, String name, String type, boolean allVersions) {
		List<UserFileNoData> fl = filesRepository.listFilesByUserid(dev.userid, name, type);
		UserFilesResults res = new UserFilesResults();
		res.maximumAccountSize = MAXIMUM_ACCOUNT_SIZE;
		res.uniqueFiles = new ArrayList<>();
		if (allVersions) {
			res.allFiles = new ArrayList<>();
		}
		res.deviceid = dev.id;
		Set<String> fileIds = new TreeSet<String>();
		for (UserFileNoData sf : fl) {
			String fileId = sf.type + "____" + sf.name;
			if (sf.filesize >= 0) {
				res.totalFileVersions++;
				res.totalZipSize += sf.zipSize;
				res.totalFileSize += sf.filesize;
			}
			if (fileIds.add(fileId)) {
				if (sf.filesize >= 0) {
					res.totalFiles++;
					res.uniqueFiles.add(sf);
				}
			}
			if (allVersions) {
				res.allFiles.add(sf);

			}
		}
		return res;
	}

	public static class UserFilesResults {
		public int totalZipSize;
		public int totalFileSize;
		public int totalFiles;
		public int totalFileVersions;
		public List<UserFileNoData> allFiles;
		public List<UserFileNoData> uniqueFiles;
		public int deviceid;
		public long maximumAccountSize;

	}
}