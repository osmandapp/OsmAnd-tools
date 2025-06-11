package net.osmand.server.api.services;

import java.io.IOException;
import java.util.*;

import com.google.gson.Gson;
import net.osmand.purchases.*;
import net.osmand.server.PurchasesDataLoader;
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
	
	private static final String GOOGLE_PACKAGE_NAME = UpdateSubscription.GOOGLE_PACKAGE_NAME;
	private static final String GOOGLE_PACKAGE_NAME_FREE = UpdateSubscription.GOOGLE_PACKAGE_NAME_FREE;
	public static final String OSMAND_PRO_IOS_SUBSCRIPTION = UpdateSubscription.OSMAND_PRO_IOS_SUBSCRIPTION_PREFIX;

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
	private static final String STATE_KEY = "state";
	private static final String PURCHASE_TIME_KEY = "purchaseTime";
	private static final String SUBSCRIPTIONS_KEY = "subscriptions";
	private static final String IN_APP_PURCHASES_KEY = "inAppPurchases";

	private static final Log LOG = LogFactory.getLog(UserSubscriptionService.class);

	@Value("${google.androidPublisher.clientSecret}")
	protected String clientSecretFile;

	@Autowired
	protected DeviceSubscriptionsRepository subscriptionsRepo;

	@Autowired
	protected DeviceInAppPurchasesRepository inAppPurchasesRepo;

	@Autowired
	protected CloudUsersRepository usersRepository;

	@Autowired
	protected PurchasesDataLoader purchasesDataLoader;

	Gson gson = new Gson();

	private AndroidPublisher androidPublisher;
	private HuaweiIAPHelper huaweiIAPHelper;
	private AmazonIAPHelper amazonIAPHelper;


	// returns null if ok
	public String checkOrderIdPro(String orderid) {
		Map<String, PurchasesDataLoader.InApp> inappMap = purchasesDataLoader.getInApps();
		Map<String, PurchasesDataLoader.Subscription> subMap = purchasesDataLoader.getSubscriptions();
		if (Algorithms.isEmpty(orderid)) {
			return null;
		}
		String errorMsg;
		String subErr = isProSubscriptionValid(orderid, subMap);
		if (subErr == null) {
			return null;
		}
		errorMsg = subErr;
		String inappErr = isProInappValid(orderid, inappMap);
		if (inappErr != null) {
			errorMsg += "; " + inappErr;
		}
		return errorMsg;
	}

	private String isProSubscriptionValid(String orderid, Map<String, PurchasesDataLoader.Subscription> subMap) {
		List<SupporterDeviceSubscription> lst = subscriptionsRepo.findByOrderId(orderid);
		for (SupporterDeviceSubscription s : lst) {
			// s.sku could be checked for pro
			if (s.expiretime == null || s.expiretime.getTime() < System.currentTimeMillis() || s.checktime == null) {
				PurchasesDataLoader.Subscription subBaseData = subMap.get(s.sku);
				if (subBaseData == null) {
					LOG.info("No subscription data found for sku: " + s.sku);
					return "subscription data not found";
				}
				if (!subBaseData.hasPro()) {
					return "subscription is not eligible for OsmAnd Cloud";
				}
				if (s.sku.contains(OSMAND_PROMO_SUBSCRIPTION)) {
					// no need to revalidate
				} else {
					if (subBaseData.platform().equalsIgnoreCase(PLATFORM_GOOGLE)) {
						s = revalidateGoogleSubscription(s);
					} else if (subBaseData.platform().equalsIgnoreCase(PLATFORM_HUAWEI)) {
						s = revalidateHuaweiSubscription(s);
					} else if (subBaseData.platform().equalsIgnoreCase(PLATFORM_AMAZON)) {
						s = revalidateAmazonSubscription(s);
					} else if (subBaseData.platform().equalsIgnoreCase(PLATFORM_APPLE)) {
						s = revalidateiOSSubscription(s);
					} else if (subBaseData.platform().equalsIgnoreCase(PLATFORM_FASTSPRING)) {
						s = revalidateFastSpringSubscription(s);
					} else {
						return "subscription is not eligible for OsmAnd Cloud";
					}
				}
			}
			if (s.valid == null || !s.valid) {
				return "no valid subscription present";
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

	private String isProInappValid(String orderid, Map<String, PurchasesDataLoader.InApp> inappMap) {
		List<SupporterDeviceInAppPurchase> lst = inAppPurchasesRepo.findByOrderId(orderid);
		return lst.stream()
				.findFirst()
				.map(p -> isProInappValid(p, inappMap))
				.orElse("no inapp purchase present");
	}

	public String isProInappValid(SupporterDeviceInAppPurchase p, Map<String, PurchasesDataLoader.InApp> inappMap) {
		PurchasesDataLoader.InApp inAppBaseData = inappMap.get(p.sku);
		if (inAppBaseData == null) {
			LOG.info("No in-app purchase data found for sku: " + p.sku);
			return "inapp purchase data not found";
		}

		int years;
		if (inAppBaseData.getProFeatures() != null && inAppBaseData.getProFeatures().expire() != null) {
			String exp = inAppBaseData.getProFeatures().expire();
			if (exp.endsWith("y")) {
				years = Integer.parseInt(exp.substring(0, exp.length() - 1));
			} else {
				years = Integer.parseInt(exp);
			}
		} else {
			return "inapp is not eligible for OsmAnd Cloud";
		}
		if (p.valid == null || !p.valid) {
			return "no valid inapp purchase present";
		} else {
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(p.purchaseTime);
			calendar.add(Calendar.YEAR, years);
			Date expireTime = calendar.getTime();
			if (expireTime.getTime() > System.currentTimeMillis()) {
				return null;
			} else {
				return "inapp purchase is expired or not validated yet";
			}
		}
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
					subOrderId = subscription.orderId;
				}
			}
		}
		String inappOrderId = null;
		List<SupporterDeviceInAppPurchase> inApps = inAppPurchasesRepo.findByOrderId(pu.orderid);
		Map<String, PurchasesDataLoader.InApp> inappMap = purchasesDataLoader.getInApps();
		if (inApps != null && !inApps.isEmpty()) {
			for (SupporterDeviceInAppPurchase p : inApps) {
				if (isProInappValid(p, inappMap) == null) {
					subOrderId = p.orderId;
				}
			}
		}
		if (subOrderId != null) {
			pu.orderid = inappOrderId;
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
		Map<String, PurchasesDataLoader.InApp> inappMap = purchasesDataLoader.getInApps();
		if (inAppPurchases != null && !inAppPurchases.isEmpty()) {
			inAppPurchases.forEach(s -> {
				if (s.userId == null && isProInappValid(s, inappMap) == null) {
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
		subMap.put(STATE_KEY, state);
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
			inAppMap.put(PURCHASE_TIME_KEY, String.valueOf(inAppPurchase.purchaseTime.getTime()));
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
		return parsePlatform(platform);
	}

	private String parsePlatform(String platform) {
		if (platform == null) {
			return "Other";
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

	public Map<String, String> getUserAccountInfo(CloudUsersRepository.CloudUser pu, String errorMsgOrderId) {
		Map<String, PurchasesDataLoader.Subscription> subMap = purchasesDataLoader.getSubscriptions();
		Map<String, PurchasesDataLoader.InApp> inappMap = purchasesDataLoader.getInApps();
		Map<String, String> info = new HashMap<>();
		String orderId = pu.orderid;
		if (orderId == null || errorMsgOrderId != null) {
			info.put(ACCOUNT_KEY, FREE_ACCOUNT);
			info.put(MAX_ACCOUNT_SIZE, String.valueOf((MAXIMUM_FREE_ACCOUNT_SIZE)));
		} else {
			List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions = subscriptionsRepo.findByOrderId(orderId);
			if (!subscriptions.isEmpty()) {
				// get main pro subscription
				DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription = subscriptions.get(0);
				if (subscriptions.size() > 1) {
					subscription = subscriptions.stream()
							.filter(s -> Boolean.TRUE.equals(s.valid))
							.findFirst()
							.orElse(subscription);
				}

				Map<String, String> subInfo = getSubscriptionInfo(subscription);
				info.putAll(subInfo);

				PurchasesDataLoader.Subscription subBaseData = subMap.get(subscription.sku);
				if (subBaseData == null) {
					LOG.info("No subscription data found for sku: " + subscription.sku);
					info.put(PURCHASE_NAME_KEY, getSubscriptionName(subscription));
				} else {
					info.put(PURCHASE_NAME_KEY, subBaseData.name());
				}
				info.put(MAX_ACCOUNT_SIZE, String.valueOf((MAXIMUM_ACCOUNT_SIZE)));
			}
			// get other purchases
			info.put(SUBSCRIPTIONS_KEY, gson.toJson(getAllSubscriptionsInfo(pu, subMap)));
			info.put(IN_APP_PURCHASES_KEY, gson.toJson(getAllInAppsInfo(pu, inappMap)));
		}
		return info;
	}

	private List<Map<String, String>> getAllSubscriptionsInfo(CloudUsersRepository.CloudUser pu, Map<String, PurchasesDataLoader.Subscription> subMap) {
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
				PurchasesDataLoader.Subscription subBaseData = subMap.get(s.sku);
				if (subBaseData != null && !subBaseData.isCrossPlatform()) {
					return; // skip non-cross-platform subscriptions
				}
				Map<String, String> subInfo = getSubscriptionInfo(s);
				if (subBaseData == null) {
					LOG.info("No subscription data found for sku: " + s.sku);
					subInfo.put(PURCHASE_NAME_KEY, getSubscriptionName(s));
					subInfo.put(PURCHASE_TYPE_KEY, getSubscriptionType(s));
					subInfo.put(PURCHASE_STORE_KEY, getSubscriptionStore(s));
				} else {
					subInfo.put(PURCHASE_NAME_KEY, subBaseData.name());
					subInfo.put(PURCHASE_TYPE_KEY, subBaseData.duration() >= 12 ? "annual" : "monthly");
					subInfo.put(PURCHASE_STORE_KEY, parsePlatform(subBaseData.platform()));
				}
				subInfo.put(BILLING_DATE_KEY, getSubscriptionBillingDate(s));
				subsInfo.add(subInfo);
			});
		}
		return subsInfo;
	}

	private List<Map<String, String>> getAllInAppsInfo(CloudUsersRepository.CloudUser pu, Map<String, PurchasesDataLoader.InApp> inappMap) {
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
				PurchasesDataLoader.InApp inAppBaseData = inappMap.get(p.sku);
				if (inAppBaseData != null && !inAppBaseData.isCrossPlatform()) {
					return; // skip non-cross-platform in-app purchases
				}
				Map<String, String> pInfo = getInAppInfo(p);
				if (inAppBaseData == null) {
					LOG.info("No in-app purchase data found for sku: " + p.sku);
					pInfo.put(PURCHASE_NAME_KEY, getInAppName(p));
					pInfo.put(PURCHASE_STORE_KEY, getInAppStore(p));
				} else {
					pInfo.put(PURCHASE_NAME_KEY, inAppBaseData.name());
					pInfo.put(PURCHASE_STORE_KEY, parsePlatform(inAppBaseData.platform()));
				}
				inAppPurchasesInfo.add(pInfo);
			});
		}
		return inAppPurchasesInfo;
	}

}
