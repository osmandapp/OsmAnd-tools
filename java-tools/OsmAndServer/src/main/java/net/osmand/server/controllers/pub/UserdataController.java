package net.osmand.server.controllers.pub;

import static org.springframework.http.MediaType.APPLICATION_OCTET_STREAM;
import static org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import com.google.api.services.androidpublisher.model.IntroductoryPriceInfo;
import com.google.gson.JsonObject;
import net.osmand.live.subscriptions.ReceiptValidationHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.AndroidPublisher.Purchases.Subscriptions;
import com.google.api.services.androidpublisher.model.SubscriptionPurchase;
import com.google.gson.Gson;

import net.osmand.live.subscriptions.AmazonIAPHelper;
import net.osmand.live.subscriptions.AmazonIAPHelper.AmazonSubscription;
import net.osmand.live.subscriptions.HuaweiIAPHelper;
import net.osmand.live.subscriptions.HuaweiIAPHelper.HuaweiSubscription;
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
import net.osmand.server.api.services.DownloadIndexesService;
import net.osmand.server.api.services.EmailSenderService;
import net.osmand.server.api.services.StorageService;
import net.osmand.util.Algorithms;

@RestController
@RequestMapping("/userdata")
public class UserdataController {

	public static final String TOKEN_DEVICE_WEB = "web";
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
	private static final int ERROR_CODE_PASSWORD_IS_TO_SIMPLE = 12 + ERROR_CODE_PREMIUM_USERS;

	protected static final Log LOG = LogFactory.getLog(UserdataController.class);

	private static final String OSMAND_PRO_ANDROID_SUBSCRIPTION = UpdateSubscription.OSMAND_PRO_ANDROID_SUBSCRIPTION_PREFIX;
	private static final String OSMAND_PROMO_SUBSCRIPTION = "promo_";
	private static final String GOOGLE_PACKAGE_NAME = UpdateSubscription.GOOGLE_PACKAGE_NAME;
	private static final String GOOGLE_PACKAGE_NAME_FREE = UpdateSubscription.GOOGLE_PACKAGE_NAME_FREE;

	private static final String OSMAND_PRO_HUAWEI_SUBSCRIPTION_1 = UpdateSubscription.OSMAND_PRO_HUAWEI_SUBSCRIPTION_PART_M;
	private static final String OSMAND_PRO_HUAWEI_SUBSCRIPTION_2 = UpdateSubscription.OSMAND_PRO_HUAWEI_SUBSCRIPTION_PART_Y;
	private static final String OSMAND_PRO_AMAZON_SUBSCRIPTION = UpdateSubscription.OSMAND_PRO_AMAZON_SUBSCRIPTION_PART;
	private static final String OSMAND_PRO_IOS_SUBSCRIPTION = UpdateSubscription.OSMAND_PRO_IOS_SUBSCRIPTION_PREFIX;

	Gson gson = new Gson();

	@Autowired
	PasswordEncoder encoder;

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
	protected DownloadIndexesService downloadService;

	@Autowired
	EmailSenderService emailSender;

	@Value("${google.androidPublisher.clientSecret}")
	protected String clientSecretFile;

	private AndroidPublisher androidPublisher;
	private HuaweiIAPHelper huaweiIAPHelper;
	private AmazonIAPHelper amazonIAPHelper;

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
					// watch first time you need to open special url on web server
					this.androidPublisher = UpdateSubscription.getPublisherApi(clientSecretFile);
				} catch (Exception e) {
					LOG.error("Error configuring android publisher api: " + e.getMessage(), e);
				}
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
						// s.checktime = new Date(); // don't set checktime let jenkins do its job
						s.valid = System.currentTimeMillis() < subscription.getExpiryTimeMillis();
						subscriptionsRepo.save(s);
					}
				}
			} catch (IOException e) {
				LOG.error(String.format("Error retrieving android publisher subscription %s - %s: %s",
						s.sku, s.orderId, e.getMessage()), e);
			}
		}
		return s;
	}

	private SupporterDeviceSubscription revalidateHuaweiSubscription(SupporterDeviceSubscription s) {
		if (huaweiIAPHelper == null) {
			huaweiIAPHelper = new HuaweiIAPHelper();
		}
		try {
			if (s.orderId == null) {
				return s;
			}
			HuaweiSubscription subscription = huaweiIAPHelper.getHuaweiSubscription(s.orderId, s.purchaseToken);
			if (subscription != null) {
				if (s.expiretime == null || s.expiretime.getTime() < subscription.expirationDate) {
					s.expiretime = new Date(subscription.expirationDate);
					// s.checktime = new Date(); // don't set checktime let jenkins do its job
					s.valid = System.currentTimeMillis() < subscription.expirationDate;
					subscriptionsRepo.save(s);
				}
			}
		} catch (IOException e) {
			LOG.error(String.format("Error retrieving huawei subscription %s - %s: %s",
					s.sku, s.orderId, e.getMessage()), e);
		}
		return s;
	}

	private SupporterDeviceSubscription revalidateAmazonSubscription(SupporterDeviceSubscription s) {
		if (amazonIAPHelper == null) {
			amazonIAPHelper = new AmazonIAPHelper();
		}
		try {
			if (s.orderId == null) {
				return s;
			}
			AmazonSubscription subscription = amazonIAPHelper.getAmazonSubscription(s.orderId, s.purchaseToken);
			if (subscription != null) {
				if (s.expiretime == null || s.expiretime.getTime() < subscription.renewalDate) {
					s.expiretime = new Date(subscription.renewalDate);
					// s.checktime = new Date(); // don't set checktime let jenkins do its job
					s.valid = System.currentTimeMillis() < subscription.renewalDate;
					subscriptionsRepo.save(s);
				}
			}
		} catch (IOException e) {
			LOG.error(String.format("Error retrieving amazon subscription %s - %s: %s",
					s.sku, s.orderId, e.getMessage()), e);
		}
		return s;
	}

    private JsonObject revalidateiOSSubscription(String orderid) {
        String errorMsg = "";
        List<SupporterDeviceSubscription> lst = subscriptionsRepo.findByOrderId(orderid);
        SupporterDeviceSubscription result = null;
        for (SupporterDeviceSubscription s : lst) {
            if (!s.sku.startsWith(OSMAND_PRO_IOS_SUBSCRIPTION)) {
                errorMsg = "subscription has not iOS sku " + s.sku;
                continue;
            }
            // s.sku could be checked for premium
            if (s.expiretime == null || s.expiretime.getTime() < System.currentTimeMillis() || s.checktime == null) {
                ReceiptValidationHelper receiptValidationHelper = new ReceiptValidationHelper();
                String purchaseToken = s.purchaseToken;
                try {
                    ReceiptValidationHelper.ReceiptResult loadReceipt = receiptValidationHelper.loadReceipt(purchaseToken);
                    if (loadReceipt.result) {
                        JsonObject receiptObj = loadReceipt.response;
                        SubscriptionPurchase subscription = null;
                        subscription = UpdateSubscription.parseIosSubscription(s.sku, s.orderId, s.introcycles == null ? 0 : s.introcycles, receiptObj);
                        if (subscription != null) {
                            if (s.expiretime == null || s.expiretime.getTime() < subscription.getExpiryTimeMillis()) {
                                s.expiretime = new Date(subscription.getExpiryTimeMillis());
                                // s.checktime = new Date(); // don't set checktime let jenkins do its job
                                s.valid = System.currentTimeMillis() < subscription.getExpiryTimeMillis();
                                subscriptionsRepo.save(s);
                            }
                            result = s;
                            errorMsg = "";
                            break;
                        } else {
                            errorMsg = "no purchases purchase format";
                        }
                    } else if (loadReceipt.error == ReceiptValidationHelper.USER_GONE) {
                        errorMsg = "Apple code:" + loadReceipt.error + " The user account cannot be found or has been deleted";
                    } else if (loadReceipt.error == ReceiptValidationHelper.SANDBOX_ERROR_CODE_TEST) {
                        errorMsg = "Apple code:" + loadReceipt.error + " This receipt is from the test environment";
                    } else {
                        errorMsg = "Apple code:" + loadReceipt.error + " See code status https://developer.apple.com/documentation/appstorereceipts/status";
                    }
                } catch (RuntimeException e) {
                    errorMsg = e.getMessage();
                }
            } else {
                result = s;
                errorMsg = "";
                break;
            }
        }
        if (errorMsg.isEmpty()) {
            if (result == null || result.valid == null || !result.valid) {
                errorMsg = "no valid subscription present";
            } else if (!result.sku.startsWith(OSMAND_PRO_IOS_SUBSCRIPTION)) {
                errorMsg = "subscription has not iOS sku " + result.sku;
            } else if (result.expiretime == null || result.expiretime.getTime() < System.currentTimeMillis()) {
                errorMsg = "subscription is expired or not validated yet";
            }
        }
        JsonObject json = new JsonObject();
        if (errorMsg.isEmpty()) {
            json.addProperty("sku", result.sku);
            json.addProperty("starttime", result.starttime.getTime());
            json.addProperty("expiretime", result.expiretime.getTime());
        } else {
            json.addProperty("error", errorMsg);
        }
        return json;
    }

	private String checkOrderIdPremium(String orderid) {
		if (Algorithms.isEmpty(orderid)) {
			return "no subscription provided";
		}
		String errorMsg = "no subscription present";
		List<SupporterDeviceSubscription> lst = subscriptionsRepo.findByOrderId(orderid);
		for (SupporterDeviceSubscription s : lst) {
			// s.sku could be checked for premium
			if (s.expiretime == null || s.expiretime.getTime() < System.currentTimeMillis() || s.checktime == null) {
				if (s.sku.startsWith(OSMAND_PRO_ANDROID_SUBSCRIPTION)) {
					s = revalidateGoogleSubscription(s);
				} else if (s.sku.contains(OSMAND_PRO_HUAWEI_SUBSCRIPTION_1) || s.sku.contains(OSMAND_PRO_HUAWEI_SUBSCRIPTION_2)) {
					s = revalidateHuaweiSubscription(s);
				} else if (s.sku.contains(OSMAND_PRO_AMAZON_SUBSCRIPTION)) {
					s = revalidateAmazonSubscription(s);
				}
			}
			if (s.valid == null || !s.valid.booleanValue()) {
				errorMsg = "no valid subscription present";
			} else if (!s.sku.startsWith(OSMAND_PRO_ANDROID_SUBSCRIPTION) &&
					!s.sku.startsWith(OSMAND_PROMO_SUBSCRIPTION) &&
					!s.sku.contains(OSMAND_PRO_HUAWEI_SUBSCRIPTION_1) &&
					!s.sku.contains(OSMAND_PRO_HUAWEI_SUBSCRIPTION_2) &&
					!s.sku.contains(OSMAND_PRO_AMAZON_SUBSCRIPTION)) {
				errorMsg = "subscription is not eligible for OsmAnd Cloud";
			} else {
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

	@GetMapping(value = "/user-validate-sub")
	@ResponseBody
	public ResponseEntity<String> check(@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken) throws IOException {
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		if (dev == null) {
			return tokenNotValid();
		}
		PremiumUser pu = usersRepository.findById(dev.userid);
		if (pu == null) {
			return error(ERROR_CODE_EMAIL_IS_INVALID, "email is not registered");
		}
		String errorMsg = checkOrderIdPremium(pu.orderid);
		if (errorMsg != null) {
			return error(ERROR_CODE_NO_VALID_SUBSCRIPTION, errorMsg);
		}
		return ResponseEntity.ok(gson.toJson(pu));
	}

    @GetMapping(value = "/user-validate-ios-sub")
    @ResponseBody
    public ResponseEntity<String> checkOrderid(@RequestParam(name = "orderid", required = true) String orderid) throws IOException {
        JsonObject json = revalidateiOSSubscription(orderid);
        if (json.has("error")) {
            return error(ERROR_CODE_NO_VALID_SUBSCRIPTION, json.get("error").getAsString());
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
			return error(ERROR_CODE_EMAIL_IS_INVALID, "email is not registered");
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

	public ResponseEntity<String> webUserActivate(String email, String token, String password) throws IOException {
		if (password.length() < 6) {
			return error(ERROR_CODE_PASSWORD_IS_TO_SIMPLE, "enter password with at least 6 symbols");
		}
		return registerNewDevice(email, token, TOKEN_DEVICE_WEB, encoder.encode(password));
	}

	public ResponseEntity<String> webUserRegister(@RequestParam(name = "email", required = true) String email) throws IOException {
		// allow to register only with small case
		email = email.toLowerCase().trim();
		if (!email.contains("@")) {
			return error(ERROR_CODE_EMAIL_IS_INVALID, "email is not valid to be registered");
		}
		PremiumUser pu = usersRepository.findByEmail(email);
		if (pu == null) {
			return error(ERROR_CODE_EMAIL_IS_INVALID, "email is not registered");
		}
		String errorMsg = checkOrderIdPremium(pu.orderid);
		if (errorMsg != null) {
			return error(ERROR_CODE_NO_VALID_SUBSCRIPTION, errorMsg);
		}
		pu.tokendevice = TOKEN_DEVICE_WEB;
		pu.token = (new Random().nextInt(8999) + 1000) + "";
		pu.tokenTime = new Date();
		usersRepository.saveAndFlush(pu);
		emailSender.sendOsmAndCloudWebEmail(pu.email, pu.token);
		return ok();
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
			return error(ERROR_CODE_EMAIL_IS_INVALID, "email is not valid to be registered");
		}
		if (pu != null) {
			if (!login) {
				return error(ERROR_CODE_USER_IS_ALREADY_REGISTERED, "user was already registered with such email");
			}
			// don't check order id validity for login
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
		emailSender.sendOsmAndCloudRegistrationEmail(pu.email, pu.token, true);
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
		String accessToken = UUID.randomUUID().toString();
		return registerNewDevice(email, token, deviceId, accessToken);
	}

	private ResponseEntity<String> registerNewDevice(String email, String token, String deviceId, String accessToken) {
		email = email.toLowerCase().trim();
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
		PremiumUserDevice sameDevice ;
		while ((sameDevice = devicesRepository.findTopByUseridAndDeviceidOrderByUdpatetimeDesc(pu.id,
				deviceId)) != null) {
			devicesRepository.delete(sameDevice);
		}
		device.userid = pu.id;
		device.deviceid = deviceId;
		device.udpatetime = new Date();
		device.accesstoken = accessToken;
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

	private MultipartFile createEmptyMultipartFile(MultipartFile file) {
		return new MultipartFile() {

			@Override
			public void transferTo(File dest) throws IOException, IllegalStateException {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean isEmpty() {
				return getSize() == 0;
			}

			@Override
			public long getSize() {
				try {
					return getBytes().length;
				} catch (IOException e) {
					return 0;
				}
			}

			@Override
			public String getOriginalFilename() {
				return file.getOriginalFilename();
			}

			@Override
			public String getName() {
				return file.getName();
			}

			@Override
			public InputStream getInputStream() throws IOException {
				return new ByteArrayInputStream(getBytes());
			}

			@Override
			public String getContentType() {
				return file.getContentType();
			}

			@Override
			public byte[] getBytes() throws IOException {
				byte[] bytes = "{\"type\":\"obf\"}".getBytes();
				ByteArrayOutputStream bous = new ByteArrayOutputStream();
				GZIPOutputStream gz = new GZIPOutputStream(bous);
				Algorithms.streamCopy(new ByteArrayInputStream(bytes), gz);
				gz.close();
				return bous.toByteArray();
			}
		};
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
		UserFilesResults res = generateFiles(dev.userid, null, null, false, false);
		if (res.totalZipSize > MAXIMUM_ACCOUNT_SIZE) {
			return error(ERROR_CODE_SIZE_OF_SUPPORTED_BOX_IS_EXCEEDED,
					"Maximum size of OsmAnd Cloud exceeded " + (MAXIMUM_ACCOUNT_SIZE / MB)
							+ " MB. Please contact support in order to investigate possible solutions.");
		}
		UserFile usf = new PremiumUserFilesRepository.UserFile();
		long cnt, sum;
		boolean checkExistingServerMap = checkThatObfFileisOnServer(name, type);
		if (checkExistingServerMap) {
			file = createEmptyMultipartFile(file);
		}
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

	@GetMapping(value = "/check-file-on-server")
	@ResponseBody
	public ResponseEntity<String> upload(@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type) throws IOException {
		if (checkThatObfFileisOnServer(name, type)) {
			return ResponseEntity.ok(gson.toJson(Collections.singletonMap("status", "present")));
		}
		return ResponseEntity.ok(gson.toJson(Collections.singletonMap("status", "not-present")));
	}

	private boolean checkThatObfFileisOnServer(String name, String type) throws IOException {
		boolean checkExistingServerMap = type.toLowerCase().equals("file") && name.endsWith(".obf");
		if (checkExistingServerMap) {
			File fp = downloadService.getFilePath(name);
			if (fp == null) {
				LOG.info("File is not found: " + name);
				checkExistingServerMap = false;
			}
		}
		return checkExistingServerMap;
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
		PremiumUserDevice dev = checkToken(deviceId, accessToken);
		getFile(response, request, name, type, updatetime, dev);
	}

	public void getFile(HttpServletResponse response, HttpServletRequest request, String name, String type,
			Long updatetime, PremiumUserDevice dev) throws IOException {
		InputStream bin = null;
		try {
			@SuppressWarnings("unchecked")
			ResponseEntity<String>[] error = new ResponseEntity[] { null };
			UserFile userFile = getUserFile(name, type, updatetime, dev);
			boolean checkExistingServerMap = type.toLowerCase().equals("file") && name.endsWith(".obf");
			boolean gzin = true, gzout;
			if(userFile.filesize < 1000 && checkExistingServerMap) {
				// file is not stored here
				File fp = downloadService.getFilePath(name);
				if (fp != null) {
					gzin = false;
					bin = getGzipInputStreamFromFile(fp, ".obf");
				}
				if (bin == null) {
					error[0] = error(ERROR_CODE_FILE_NOT_AVAILABLE, "File is not available");
				}
			} else {
				bin = getInputStream(dev, error, userFile);
			}
			if (error[0] != null) {
				response.setStatus(error[0].getStatusCodeValue());
				response.getWriter().write(error[0].getBody());
				return;
			}
			response.setHeader("Content-Disposition", "attachment; filename=" + userFile.name);
			// InputStream bin = fl.data.getBinaryStream();

			String acceptEncoding = request.getHeader("Accept-Encoding");
			if (acceptEncoding != null && acceptEncoding.contains("gzip")) {
				response.setHeader("Content-Encoding", "gzip");
				gzout = true;
			} else {
				if (gzin) {
					bin = new GZIPInputStream(bin);
				}
				gzout = false;
			}
			response.setContentType(APPLICATION_OCTET_STREAM.getType());
			byte[] buf = new byte[BUFFER_SIZE];
			int r;
			OutputStream ous = gzout && !gzin ? new GZIPOutputStream(response.getOutputStream()) : response.getOutputStream();
			while ((r = bin.read(buf)) != -1) {
				ous.write(buf, 0, r);
			}
			ous.close();
		} finally {
			if (bin != null) {
				bin.close();
			}
		}
	}

	private InputStream getGzipInputStreamFromFile(File fp, String ext) throws IOException {
		if (fp.getName().endsWith(".zip")) {
			ZipInputStream zis = new ZipInputStream(new FileInputStream(fp));
			ZipEntry ze = zis.getNextEntry();
			while (ze != null && !ze.getName().endsWith(ext)) {
				ze = zis.getNextEntry();
			}
			if (ze != null) {
				return zis;
			}
		}
		return new FileInputStream(fp);
	}


	public InputStream getInputStream(PremiumUserDevice dev, ResponseEntity<String>[] error, UserFile userFile) {
		InputStream bin = null;
		if (dev == null) {
			error[0] = tokenNotValid();
		} else {
			if (userFile == null) {
				error[0] = error(ERROR_CODE_FILE_NOT_AVAILABLE, "File is not available");
			} else if (userFile.data == null) {
				bin = getInputStream(userFile);
				if (bin == null) {
					error[0] = error(ERROR_CODE_FILE_NOT_AVAILABLE, "File is not available");
				}
			} else {
				bin = new ByteArrayInputStream(userFile.data);
			}
		}
		return bin;
	}

	public UserFile getUserFile(String name, String type, Long updatetime, PremiumUserDevice dev) {
		if (dev == null) {
			return null;
		}
		if (updatetime != null) {
			return filesRepository.findTopByUseridAndNameAndTypeAndUpdatetime(dev.userid, name, type,
					new Date(updatetime));
		} else {
			return filesRepository.findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(dev.userid, name, type);
		}
	}

	public InputStream getInputStream(UserFile userFile) {
		return storageService.getFileInputStream(userFile.storage, userFolder(userFile), storageFileName(userFile));
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
		UserFilesResults res = generateFiles(dev.userid, name, type, allVersions, false);
		return ResponseEntity.ok(gson.toJson(res));
	}

	public UserFilesResults generateFiles(int userId, String name, String type, boolean allVersions, boolean details) {
		List<UserFileNoData> fl =
				details ? filesRepository.listFilesByUseridWithDetails(userId, name, type) :
						filesRepository.listFilesByUserid(userId, name, type);
		UserFilesResults res = new UserFilesResults();
		res.maximumAccountSize = MAXIMUM_ACCOUNT_SIZE;
		res.uniqueFiles = new ArrayList<>();
		if (allVersions) {
			res.allFiles = new ArrayList<>();
		}
		res.userid = userId;
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
		public int userid;
		public long maximumAccountSize;

	}
}
