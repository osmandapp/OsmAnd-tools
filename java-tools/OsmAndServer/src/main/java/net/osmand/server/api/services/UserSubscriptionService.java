package net.osmand.server.api.services;

import java.io.IOException;
import java.util.*;

import com.google.gson.Gson;
import net.osmand.purchases.*;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.AndroidPublisher.Purchases.Subscriptions;
import com.google.api.services.androidpublisher.model.SubscriptionPurchase;
import com.google.gson.JsonObject;

import net.osmand.purchases.ReceiptValidationHelper.ReceiptResult;
import net.osmand.purchases.AmazonIAPHelper.AmazonSubscription;
import net.osmand.purchases.HuaweiIAPHelper.HuaweiSubscription;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscription;
import net.osmand.util.Algorithms;

import static net.osmand.server.api.services.UserdataService.MAXIMUM_ACCOUNT_SIZE;
import static net.osmand.server.api.services.UserdataService.MAXIMUM_FREE_ACCOUNT_SIZE;
import static net.osmand.server.controllers.pub.SubscriptionController.*;

@Service
public class UserSubscriptionService {

	public static final String OSMAND_PRO_ANDROID_SUBSCRIPTION = UpdateSubscription.OSMAND_PRO_ANDROID_SUBSCRIPTION_PREFIX;
	public static final String OSMAND_PROMO_SUBSCRIPTION = "promo_";
	public static final String OSMAND_CLOUD_INAPP = "osmand_cloud_inapp_";
	private static final String GOOGLE_PACKAGE_NAME = UpdateSubscription.GOOGLE_PACKAGE_NAME;
	private static final String GOOGLE_PACKAGE_NAME_FREE = UpdateSubscription.GOOGLE_PACKAGE_NAME_FREE;

	public static final String OSMAND_PRO_HUAWEI_SUBSCRIPTION_1 = UpdateSubscription.OSMAND_PRO_HUAWEI_SUBSCRIPTION_PART_M;
	public static final String OSMAND_PRO_HUAWEI_SUBSCRIPTION_2 = UpdateSubscription.OSMAND_PRO_HUAWEI_SUBSCRIPTION_PART_Y;
	public static final String OSMAND_PRO_AMAZON_SUBSCRIPTION = UpdateSubscription.OSMAND_PRO_AMAZON_SUBSCRIPTION_PART;
	public static final String OSMAND_PRO_IOS_SUBSCRIPTION = UpdateSubscription.OSMAND_PRO_IOS_SUBSCRIPTION_PREFIX;
	public static final String OSMAND_PRO_FAST_SPRINGS_SUBSCRIPTION = UpdateSubscription.OSMAND_PRO_FAST_SPRING_SUBSCRIPTION_PREFIX;

	private static final String PLATFORM_WEB_NAME_FASTSPRING = "OsmAnd Web (FastSpring)";
	private static final String PLATFORM_WEB_NAME_GOOGLE = "Google Play";
	private static final String PLATFORM_WEB_NAME_APPLE = "Apple App Store";
	private static final String PLATFORM_WEB_NAME_HUAWEI = "Huawei AppGallery";
	private static final String PLATFORM_WEB_NAME_AMAZON = "Amazon";

	private static final String ACCOUNT_KEY = "account";
	private static final String FREE_ACCOUNT = "Free";
	private static final String MAX_ACCOUNT_SIZE = "maxAccSize";
	private static final String PURCHASE_NAME_KEY = "name";
	private static final String PURCHASE_STORE_KEY = "store";
	private static final String BILLING_DATE_KEY = "billingDate";
	private static final String PURCHASE_TYPE_KEY = "type";
	private static final String START_TIME_KEY = "start_time";
	private static final String EXPIRE_TIME_KEY = "expire_time";
	private static final String VALID_KEY = "valid";

	private static final Log LOG = LogFactory.getLog(UserSubscriptionService.class);

	@Value("${google.androidPublisher.clientSecret}")
	protected String clientSecretFile;

	@Autowired
	protected DeviceSubscriptionsRepository subscriptionsRepo;

	@Autowired
	protected DeviceInAppPurchasesRepository inAppPurchasesRepo;

	@Autowired
	protected CloudUsersRepository usersRepository;

	Gson gson = new Gson();

	private AndroidPublisher androidPublisher;
	private HuaweiIAPHelper huaweiIAPHelper;
	private AmazonIAPHelper amazonIAPHelper;


	// returns null if ok
	public String checkOrderIdPro(String orderid) {
		if (Algorithms.isEmpty(orderid)) {
			return null;
		}
		String errorMsg;
		String subErr = checkProSubscription(orderid);
		if (subErr == null) {
			return null;
		}
		errorMsg = subErr;
		String inappErr = checkProInapp(orderid);
		if (inappErr != null) {
			errorMsg += "; " + inappErr;
		}
		return errorMsg;
	}

	private String checkProSubscription(String orderid) {
		List<SupporterDeviceSubscription> lst = subscriptionsRepo.findByOrderId(orderid);
		for (SupporterDeviceSubscription s : lst) {
			// s.sku could be checked for pro
			if (s.expiretime == null || s.expiretime.getTime() < System.currentTimeMillis() || s.checktime == null) {
				if (s.sku.startsWith(OSMAND_PRO_ANDROID_SUBSCRIPTION)) {
					s = revalidateGoogleSubscription(s);
				} else if (s.sku.contains(OSMAND_PRO_HUAWEI_SUBSCRIPTION_1) || s.sku.contains(OSMAND_PRO_HUAWEI_SUBSCRIPTION_2)) {
					s = revalidateHuaweiSubscription(s);
				} else if (s.sku.contains(OSMAND_PRO_AMAZON_SUBSCRIPTION)) {
					s = revalidateAmazonSubscription(s);
				} else if (s.sku.startsWith(OSMAND_PRO_IOS_SUBSCRIPTION)) {
					s = revalidateiOSSubscription(s);
				} else if (s.sku.contains(OSMAND_PRO_FAST_SPRINGS_SUBSCRIPTION)) {
					s = revalidateFastSpringSubscription(s);
				}
			}
			if (s.valid == null || !s.valid) {
				return "no valid subscription present";
			} else if (!isProSubscription(s.sku)) {
				return "subscription is not eligible for OsmAnd Cloud";
			} else {
				if (s.expiretime != null && s.expiretime.getTime() > System.currentTimeMillis()) {
					return null;
				} else {
					return "subscription is expired or not validated yet";
				}
			}
		}
		return "no subscription present";
	}

	private boolean isProSubscription(String sku) {
		return sku.startsWith(OSMAND_PRO_ANDROID_SUBSCRIPTION) ||
				sku.startsWith(OSMAND_PROMO_SUBSCRIPTION) ||
				sku.contains(OSMAND_PRO_HUAWEI_SUBSCRIPTION_1) ||
				sku.contains(OSMAND_PRO_HUAWEI_SUBSCRIPTION_2) ||
				sku.contains(OSMAND_PRO_AMAZON_SUBSCRIPTION) ||
				sku.contains(OSMAND_PRO_IOS_SUBSCRIPTION) ||
				sku.contains(OSMAND_PRO_FAST_SPRINGS_SUBSCRIPTION);
	}

	private String checkProInapp(String orderid) {
		List<SupporterDeviceInAppPurchase> lst = inAppPurchasesRepo.findByOrderId(orderid);
		for (SupporterDeviceInAppPurchase p : lst) {
			if (!p.sku.startsWith(OSMAND_CLOUD_INAPP)) {
				return "inapp is not eligible for OsmAnd Cloud";
			}
			if (p.valid == null || !p.valid) {
				return "no valid inapp purchase present";
			} else {
				int years = Integer.parseInt(p.sku.substring(OSMAND_CLOUD_INAPP.length()));
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(p.purchaseTime);
				calendar.add(Calendar.YEAR, years);
				Date expireTime = getExpireTimeInAppCloudPurchase(p);
				if (expireTime != null && expireTime.getTime() > System.currentTimeMillis()) {
					return null;
				} else {
					return "inapp purchase is expired or not validated yet";
				}
			}
		}
		return "no inapp purchase present";
	}

	private Date getExpireTimeInAppCloudPurchase(SupporterDeviceInAppPurchase p) {
		if (p == null || p.sku == null || !p.sku.startsWith(OSMAND_CLOUD_INAPP)) {
			return null;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(p.purchaseTime);
		int years = Integer.parseInt(p.sku.substring(OSMAND_CLOUD_INAPP.length()));
		calendar.add(Calendar.YEAR, years);

		return calendar.getTime();
	}

	public String verifyAndRefreshProOrderId(CloudUsersRepository.CloudUser pu) {
		String errorMsg = checkOrderIdPro(pu.orderid);
		if (errorMsg != null) {
			boolean updated = updateSubscriptionUserId(pu);
			if (!updated) {
				updateInAppPurchaseUserId(pu);
			}
			updated = updateOrderId(pu);
			if (updated) {
				return null;
			}
		}
		return errorMsg;
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

	public SupporterDeviceSubscription revalidateiOSSubscription(SupporterDeviceSubscription s) {
		ReceiptValidationHelper receiptValidationHelper = new ReceiptValidationHelper();
		String purchaseToken = s.purchaseToken;
		try {
			ReceiptResult loadReceipt = receiptValidationHelper.loadReceipt(purchaseToken, false);
			if (loadReceipt.error == ReceiptValidationHelper.SANDBOX_ERROR_CODE_TEST) {
				loadReceipt = receiptValidationHelper.loadReceipt(purchaseToken, true);
			}
			if (loadReceipt.result) {
				JsonObject receiptObj = loadReceipt.response;
				SubscriptionPurchase subscription = null;
				subscription = UpdateSubscription.parseIosSubscription(s.sku, s.orderId,
						s.introcycles == null ? 0 : s.introcycles, receiptObj);
				if (subscription != null
						&& (s.expiretime == null || s.expiretime.getTime() < subscription.getExpiryTimeMillis())) {
					s.expiretime = new Date(subscription.getExpiryTimeMillis());
					// s.checktime = new Date(); // don't set checktime let jenkins do its job
					s.valid = System.currentTimeMillis() < subscription.getExpiryTimeMillis();
					subscriptionsRepo.save(s);
				}
			}
		} catch (RuntimeException e) {
			LOG.error(String.format("Error retrieving ios subscription %s - %s: %s", s.sku, s.orderId, e.getMessage()),
					e);
		}
		return s;
	}

	public SupporterDeviceSubscription revalidateFastSpringSubscription(SupporterDeviceSubscription s) {
		try {
			FastSpringHelper.FastSpringSubscription fsSub = FastSpringHelper.getSubscriptionByOrderIdAndSku(s.orderId, s.sku);
			if (fsSub != null) {
				if (s.expiretime == null) {
					LOG.error(String.format("FastSpring subscription %s - %s has no expiretime", s.sku, s.orderId));
				} else {
					if (s.expiretime.getTime() < fsSub.nextChargeDate) {
						s.expiretime = new Date(fsSub.nextChargeDate);
					}
				}
				s.valid = System.currentTimeMillis() < fsSub.nextChargeDate;
				subscriptionsRepo.save(s);
			}
		} catch (IOException e) {
			LOG.error(String.format("Error retrieving fastspring subscription %s - %s: %s", s.sku, s.orderId, e.getMessage()), e);
		}
		return s;
	}

	public boolean updateOrderId(CloudUsersRepository.CloudUser pu) {
		// get the latest subscription
		Date subExpire = new Date(0);
		String subOrderId = null;
		List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions = subscriptionsRepo.findAllByUserId(pu.id);
		if (subscriptions != null && !subscriptions.isEmpty()) {
			Optional<SupporterDeviceSubscription> maxExpiryValid = subscriptions.stream()
					.filter(s -> s.valid)
					.max(Comparator.comparing(
							(DeviceSubscriptionsRepository.SupporterDeviceSubscription s) ->
									s.expiretime != null ? s.expiretime : new Date(0)
					).thenComparing(
							s -> s.starttime != null ? s.starttime : new Date(0)
					));
			if (maxExpiryValid.isPresent()) {
				DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription = maxExpiryValid.get();
				if (subscription.orderId != null) {
					subExpire = subscription.expiretime;
					subOrderId = subscription.orderId;
				}
			}
		}
		// get the latest inapp purchase
		Date inappExpire = new Date(0);
		String inappOrderId = null;
		List<SupporterDeviceInAppPurchase> inApps = inAppPurchasesRepo.findByOrderId(pu.orderid);
		if (inApps != null && !inApps.isEmpty()) {
			for (SupporterDeviceInAppPurchase p : inApps) {
				if (Boolean.TRUE.equals(p.valid)
						&& p.sku.startsWith(OSMAND_CLOUD_INAPP)
						&& p.purchaseTime != null) {
					Date expire = getExpireTimeInAppCloudPurchase(p);
					if (expire.after(inappExpire)) {
						inappExpire = expire;
						inappOrderId = p.orderId;
					}
				}
			}
		}

		if (subOrderId != null || inappOrderId != null) {
			if (inappOrderId != null && inappExpire.after(subExpire)) {
				pu.orderid = inappOrderId;
			} else {
				pu.orderid = subOrderId;
			}
			usersRepository.saveAndFlush(pu);
			return true;
		}
		return false;
	}

	public boolean updateSubscriptionUserId(CloudUsersRepository.CloudUser pu) {
		if (pu.orderid == null) {
			return false;
		}
		List<SupporterDeviceSubscription> subscriptionList = subscriptionsRepo.findByOrderId(pu.orderid);
		if (subscriptionList != null && !subscriptionList.isEmpty()) {
			subscriptionList.forEach(s -> {
				if (s.userId == null) {
					s.userId = pu.id;
					subscriptionsRepo.saveAndFlush(s);
				}
			});
			return true;
		}
		return false;
	}

	public boolean updateInAppPurchaseUserId(CloudUsersRepository.CloudUser pu) {
		if (pu.orderid == null) {
			return false;
		}
		List<SupporterDeviceInAppPurchase> inAppPurchases = inAppPurchasesRepo.findByOrderId(pu.orderid);
		if (inAppPurchases != null && !inAppPurchases.isEmpty()) {
			inAppPurchases.forEach(s -> {
				if (s.userId == null && !s.sku.startsWith(OSMAND_CLOUD_INAPP)) {
					s.userId = pu.id;
					inAppPurchasesRepo.saveAndFlush(s);
				}
			});
			return true;
		}
		return false;
	}

	@NotNull
	public Map<String, String> getSubscriptionInfo(DeviceSubscriptionsRepository.SupporterDeviceSubscription sub) {
		Map<String, String> subMap = new HashMap<>();
		subMap.put("sku", sub.sku);
		if (sub.valid != null) {
			subMap.put(VALID_KEY, sub.valid.toString());
		}
		String state = "undefined";
		if (sub.starttime != null) {
			subMap.put(START_TIME_KEY, String.valueOf(sub.starttime.getTime()));
		}
		if (sub.expiretime != null) {
			long expireTimeMs = sub.expiretime.getTime();
			int paymentState = sub.paymentstate == null ? 1 : sub.paymentstate;
			boolean autoRenewing = sub.autorenewing != null && sub.autorenewing;
			if (expireTimeMs > System.currentTimeMillis()) {
				if (paymentState == 1 && autoRenewing) {
					state = "active";
				} else if (paymentState == 1) {
					state = "cancelled";
				} else if (paymentState == 0 && autoRenewing) {
					state = "in_grace_period";
				}
			} else {
				if (paymentState == 0 && autoRenewing) {
					state = "on_hold";
				} else if (paymentState == 1 && autoRenewing) {
					state = "paused";
				} else if (paymentState == 1) {
					state = "expired";
				}
			}
			subMap.put(EXPIRE_TIME_KEY, String.valueOf(expireTimeMs));
		}
		subMap.put("state", state);
		return subMap;
	}

	public String getSubscriptionName(DeviceSubscriptionsRepository.SupporterDeviceSubscription s) {
		String sku = s.sku;
		if (sku == null) {
			return null;
		}
		if (sku.contains("_live_")) {
			return "OsmAnd Live";
		}
		if (sku.contains("promo")) {
			return "OsmAnd Promo";
		}
		if (sku.contains("maps")) {
			return "OsmAnd+";
		}
		if (isProSubscription(sku)) {
			return "OsmAnd Pro";
		}
		return "Other";

	}

	public String getSubscriptionType(DeviceSubscriptionsRepository.SupporterDeviceSubscription s) {
		String sku = s.sku;
		if (sku == null) {
			return null;
		}
		if (sku.contains(".annual")) {
			return "annual";
		} else if (sku.contains(".monthly")) {
			return "monthly";
		}
		return null;
	}

	public String getSubscriptionStore(DeviceSubscriptionsRepository.SupporterDeviceSubscription s) {
		String sku = s.sku;
		if (sku == null) {
			return null;
		}
		if (sku.startsWith(OSMAND_PRO_ANDROID_SUBSCRIPTION)) {
			return PLATFORM_WEB_NAME_GOOGLE;
		} else if (sku.startsWith(OSMAND_PRO_IOS_SUBSCRIPTION)) {
			return PLATFORM_WEB_NAME_APPLE;
		} else if (sku.contains(PLATFORM_HUAWEI)) {
			return PLATFORM_WEB_NAME_HUAWEI;
		} else if (sku.contains(PLATFORM_AMAZON)) {
			return PLATFORM_WEB_NAME_AMAZON;
		} else if (sku.contains(PLATFORM_FASTSPRING)) {
			return PLATFORM_WEB_NAME_FASTSPRING;
		}
		return "Other";
	}

	public String getSubscriptionBillingDate(DeviceSubscriptionsRepository.SupporterDeviceSubscription s) {
		if (Boolean.TRUE.equals(s.autorenewing) && s.expiretime != null) {
			return String.valueOf(s.expiretime.getTime());
		}
		return null;
	}

	@NotNull
	public Map<String, String> getInAppInfo(DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase inAppPurchase) {
		Map<String, String> inAppMap = new HashMap<>();
		inAppMap.put("sku", inAppPurchase.sku);
		if (inAppPurchase.valid != null) {
			inAppMap.put(VALID_KEY, inAppPurchase.valid.toString());
		}
		if (inAppPurchase.purchaseTime != null) {
			inAppMap.put("purchaseTime", String.valueOf(inAppPurchase.purchaseTime.getTime()));
		}
		return inAppMap;
	}

	public String getInAppName(DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase inAppPurchase) {
		String sku = inAppPurchase.sku;
		if (sku == null) {
			return null;
		}
		if (sku.contains("maps")) {
			return "Maps+";
		}
		if (sku.contains("osmand_premium")) {
			return "OsmAnd Premium";
		}
		return "Other";
	}

	public String getInAppStore(DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase inAppPurchase) {
		String platform = inAppPurchase.platform;
		if (platform == null) {
			return null;
		}
		return switch (platform) {
			case PLATFORM_GOOGLE -> PLATFORM_WEB_NAME_GOOGLE;
			case PLATFORM_APPLE -> PLATFORM_WEB_NAME_APPLE;
			case PLATFORM_HUAWEI -> PLATFORM_WEB_NAME_HUAWEI;
			case PLATFORM_AMAZON -> PLATFORM_WEB_NAME_AMAZON;
			case PLATFORM_FASTSPRING -> PLATFORM_WEB_NAME_FASTSPRING;
			default -> "Other";
		};
	}

	public Map<String, String> getUserAccountInfo(CloudUsersRepository.CloudUser pu) {
		Map<String, String> info = new HashMap<>();
		String orderId = pu.orderid;
		if (orderId == null) {
			info.put(ACCOUNT_KEY, FREE_ACCOUNT);
			info.put(MAX_ACCOUNT_SIZE, String.valueOf((MAXIMUM_FREE_ACCOUNT_SIZE)));
		} else {
			List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions = subscriptionsRepo.findByOrderId(orderId);
			if (!subscriptions.isEmpty()) {
				DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription = subscriptions.get(0);
				if (subscriptions.size() > 1) {
					subscription = subscriptions.stream()
							.filter(s -> Boolean.TRUE.equals(s.valid))
							.findFirst()
							.orElse(subscription);
				}
				Map<String, String> subInfo = getSubscriptionInfo(subscription);
				info.putAll(subInfo);
				info.put(PURCHASE_NAME_KEY, getSubscriptionName(subscription));
				info.put(PURCHASE_TYPE_KEY, getSubscriptionType(subscription));
				info.put(PURCHASE_STORE_KEY, getSubscriptionStore(subscription));
				info.put(MAX_ACCOUNT_SIZE, String.valueOf((MAXIMUM_ACCOUNT_SIZE)));
			}
			info.put("subscriptions", gson.toJson(getAllSubscriptionsInfo(pu)));
			info.put("inAppPurchases", gson.toJson(getAllInAppsInfo(pu)));
		}
		return info;
	}

	private List<Map<String, String>> getAllSubscriptionsInfo(CloudUsersRepository.CloudUser pu) {
		List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptionList = subscriptionsRepo.findAllByUserId(pu.id);
		List<Map<String, String>> subsInfo = new ArrayList<>();
		if (subscriptionList != null && !subscriptionList.isEmpty()) {
			subscriptionList.sort((a, b) -> {
				if (a.starttime == null && b.starttime == null) return 0;
				if (a.starttime == null) return 1;
				if (b.starttime == null) return -1;
				return b.starttime.compareTo(a.starttime);
			});
			subscriptionList.forEach(s -> {
				Map<String, String> subInfo = getSubscriptionInfo(s);
				subInfo.put(PURCHASE_NAME_KEY, getSubscriptionName(s));
				subInfo.put(PURCHASE_TYPE_KEY, getSubscriptionType(s));
				subInfo.put(PURCHASE_STORE_KEY, getSubscriptionStore(s));
				subInfo.put(BILLING_DATE_KEY, getSubscriptionBillingDate(s));
				subsInfo.add(subInfo);
			});
		}
		return subsInfo;
	}

	private List<Map<String, String>> getAllInAppsInfo(CloudUsersRepository.CloudUser pu) {
		List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> purchases = inAppPurchasesRepo.findByUserId(pu.id);
		List<Map<String, String>> inAppPurchasesInfo = new ArrayList<>();
		if (purchases != null && !purchases.isEmpty()) {
			purchases.sort((a, b) -> {
				if (a.purchaseTime == null && b.purchaseTime == null) return 0;
				if (a.purchaseTime == null) return 1;
				if (b.purchaseTime == null) return -1;
				return b.purchaseTime.compareTo(a.purchaseTime);
			});
			purchases.forEach(p -> {
				Map<String, String> pInfo = getInAppInfo(p);
				pInfo.put(PURCHASE_NAME_KEY, getInAppName(p));
				pInfo.put(PURCHASE_STORE_KEY, getInAppStore(p));
				inAppPurchasesInfo.add(pInfo);
			});
		}
		return inAppPurchasesInfo;
	}

}
