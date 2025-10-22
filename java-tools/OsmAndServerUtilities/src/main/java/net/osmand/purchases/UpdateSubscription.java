package net.osmand.purchases;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.json.JSONException;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver.Builder;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.model.InAppProduct;
import com.google.api.services.androidpublisher.model.InappproductsListResponse;
import com.google.api.services.androidpublisher.model.IntroductoryPriceInfo;
import com.google.api.services.androidpublisher.model.SubscriptionPurchase;
import com.google.gson.JsonObject;

import net.osmand.PlatformUtil;
import net.osmand.purchases.AmazonIAPHelper.AmazonIOException;
import net.osmand.purchases.AmazonIAPHelper.AmazonSubscription;
import net.osmand.purchases.HuaweiIAPHelper.HuaweiJsonResponseException;
import net.osmand.purchases.HuaweiIAPHelper.HuaweiSubscription;
import net.osmand.purchases.ReceiptValidationHelper.InAppReceipt;
import net.osmand.purchases.ReceiptValidationHelper.ReceiptResult;
import net.osmand.util.Algorithms;

import static net.osmand.purchases.FastSpringHelper.FastSpringSubscription;


public class UpdateSubscription {


    private final static Log LOGGER = PlatformUtil.getLog(UpdateSubscription.class);

	// init one time
	public static final String GOOGLE_PRODUCT_NAME = "OsmAnd+";
	public static final String GOOGLE_PRODUCT_NAME_FREE = "OsmAnd";

	public static final String GOOGLE_PACKAGE_NAME = "net.osmand.plus";
	public static final String GOOGLE_PACKAGE_NAME_FREE = "net.osmand";
	public static final String OSMAND_PRO_ANDROID_SUBSCRIPTION_PREFIX = "osmand_pro_";
	public static final String OSMAND_PRO_HUAWEI_SUBSCRIPTION_PART_Y = ".huawei.annual.pro";
	public static final String OSMAND_PRO_HUAWEI_SUBSCRIPTION_PART_M = ".huawei.monthly.pro";
	public static final String OSMAND_PRO_AMAZON_SUBSCRIPTION_PART = ".amazon.pro";
	public static final String OSMAND_PRO_IOS_SUBSCRIPTION_PREFIX = "net.osmand.maps.subscription.pro";
	public static final String OSMAND_PRO_FAST_SPRING_SUBSCRIPTION_PREFIX = "net.osmand.fastspring.subscription.pro";

	private static final String EXPIRED_STATE = "expired";

	private static final int BATCH_SIZE = 200;
	private static final long DAY = 1000L * 60 * 60 * 24;
	private static final long HOUR = 1000L * 60 * 60;

	private static final long MINIMUM_WAIT_TO_REVALIDATE_VALID = 14 * DAY;
	private static final long MINIMUM_WAIT_TO_REVALIDATE = 12 * HOUR;
	private static final long MAX_WAITING_TIME_TO_EXPIRE = 15 * DAY;
	private static final long MAX_WAITING_TIME_TO_MAKE_INVALID = 3 * DAY;
	int changes = 0;
	int checkChanges = 0;
	int deletions = 0;
	protected String selQuery;
	protected String updQuery;
	protected String delQuery;
	protected String updCheckQuery;
	protected PreparedStatement updStat;
	protected PreparedStatement delStat;
	protected PreparedStatement updCheckStat;
	protected SubscriptionType subType;
	private AndroidPublisher publisher;

	private static class UpdateParams {
		public boolean verifyAll;
		public boolean verbose;
		public boolean forceUpdate;
		public String orderId;
	}

	public enum SubscriptionType {
		HUAWEI,
		AMAZON,
		IOS,
		ANDROID,
//		ANDROID_LEGACY,
		PROMO,
		MANUALLY_VALIDATED,
		FASTSPRING,
		UNKNOWN;

		public static SubscriptionType fromSku(String sku) {
			if (sku.contains(".huawei.")) {
				return HUAWEI;
			} else if (sku.contains(".amazon.")) {
				return AMAZON;
			} else if (sku.startsWith("net.osmand.maps.subscription.")) {
				return IOS;
			} else if (sku.startsWith("osm_live_subscription_") || sku.startsWith("osm_free_live_subscription_")) {
				// 1st gen:
				// 		osm_live_subscription_2
				// 		osm_free_live_subscription_2
				// 2nd gen:
				// 		osm_live_subscription_annual_ full_v1 / full_v2 / free_v1 / ...
				// legacy
				return ANDROID;

			} else if (sku.startsWith("osmand_pro_") || sku.startsWith("osmand_maps_")) {
				// osmand_pro_annual_full_v1
				// osmand_maps_annual_free_v1
				return ANDROID;
			} else if (sku.startsWith("promo_")) {
				return PROMO;
			} else if (sku.startsWith("net.osmand.fastspring")) {
				return FASTSPRING;
			}
			return UNKNOWN;
		}

		public static SubscriptionType getSubType(String purchaseToken, String sku) {
			if (purchaseToken != null && purchaseToken.equals("manually-validated")) {
				return MANUALLY_VALIDATED;
			}
			return SubscriptionType.fromSku(sku);
		}
	}

	public UpdateSubscription(AndroidPublisher publisher, SubscriptionType subType, boolean revalidateInvalid) {
		this.subType = subType;
		this.publisher = publisher;
		delQuery = "UPDATE supporters_device_sub SET valid = false, kind = ?, checktime = ? "
				+ "WHERE orderid = ? and sku = ?";
		updCheckQuery = "UPDATE supporters_device_sub SET checktime = ? "
				+ "WHERE orderid = ? and sku = ?";
		String requestValid = "(valid is null or valid=true)";
		if (revalidateInvalid) {
			requestValid = "(valid=false)";
		}
		selQuery = "SELECT sku, purchaseToken, orderid, prevvalidpurchasetoken, payload, checktime, timestamp, starttime, expiretime, valid, introcycles "
				+ "FROM supporters_device_sub S where " + requestValid + " order by timestamp asc";
		if (subType == SubscriptionType.IOS) {
			updQuery = "UPDATE supporters_device_sub SET "
					+ " checktime = ?, starttime = ?, expiretime = ?, autorenewing = ?, "
					+ " introcycles = ? , "
					+ " valid = ?, kind = ?, prevvalidpurchasetoken = null " + " WHERE orderid = ? and sku = ?";
		} else if (subType == SubscriptionType.HUAWEI) {
			updQuery = "UPDATE supporters_device_sub SET "
					+ " checktime = ?, starttime = ?, expiretime = ?, autorenewing = ?, "
					+ " payload = ?, price = ?, pricecurrency = ?, "
					+ " valid = ?, kind = ?, prevvalidpurchasetoken = null " + " WHERE orderid = ? and sku = ?";
		} else if (subType == SubscriptionType.AMAZON) {
			updQuery = "UPDATE supporters_device_sub SET "
					+ " checktime = ?, starttime = ?, expiretime = ?, autorenewing = ?, "
					+ " valid = ?, kind = ?, prevvalidpurchasetoken = null " + " WHERE orderid = ? and sku = ?";
		} else if (subType == SubscriptionType.FASTSPRING) {
				updQuery = "UPDATE supporters_device_sub SET "
						+ " checktime = ?, starttime = ?, expiretime = ?, autorenewing = ?, "
						+ " price = ?, pricecurrency = ?, valid = ?, kind = ?, prevvalidpurchasetoken = null " + " WHERE orderid = ? and sku = ?";
		} else {
			updQuery = "UPDATE supporters_device_sub SET "
					+ " checktime = ?, starttime = ?, expiretime = ?, autorenewing = ?, "
					+ " paymentstate = ?, payload = ?, "
					+ " price = ?, pricecurrency = ?, introprice = ?, intropricecurrency = ?, introcycles = ? , introcyclename = ?, "
					+ " valid = ?, kind = ?, prevvalidpurchasetoken = null " + " WHERE orderid = ? and sku = ?";
		}
	}

	public static void main(String[] args) throws JSONException, IOException, SQLException, ClassNotFoundException, GeneralSecurityException {
		EnumSet<SubscriptionType> set = EnumSet.of(
				SubscriptionType.ANDROID,
				SubscriptionType.IOS,
				SubscriptionType.HUAWEI,
				SubscriptionType.AMAZON,
				SubscriptionType.FASTSPRING,
				SubscriptionType.PROMO);

		boolean revalidateinvalid = false;
		UpdateParams up = new UpdateParams();
		String androidClientSecretFile = "";
		for (int i = 0; i < args.length; i++) {
			if ("-verifyall".equals(args[i])) {
				up.verifyAll = true;
			} else if ("-verbose".equals(args[i])) {
				up.verbose = true;
			} else if (args[i].startsWith("-androidclientsecret=")) {
				androidClientSecretFile = args[i].substring("-androidclientsecret=".length());
			} else if ("-onlyandroid".equals(args[i])) {
				set = EnumSet.of(SubscriptionType.ANDROID);
			} else if ("-revalidateinvalid".equals(args[i])) {
				revalidateinvalid = true;
			} else if ("-onlyios".equals(args[i])) {
				set = EnumSet.of(SubscriptionType.IOS);
			} else if ("-onlyhuawei".equals(args[i])) {
				set = EnumSet.of(SubscriptionType.HUAWEI);
			} else if ("-onlyamazon".equals(args[i])) {
				set = EnumSet.of(SubscriptionType.AMAZON);
			} else if ("-onlyfastspring".equals(args[i])) {
				set = EnumSet.of(SubscriptionType.FASTSPRING);
			} else if ("-forceupdate".equals(args[i])) {
				up.forceUpdate = true;
			} else if (args[i].startsWith("-orderid=")) {
				up.orderId = args[i].substring("-orderid=".length());
			}
		}
		AndroidPublisher publisher = !Algorithms.isEmpty(androidClientSecretFile) ? getPublisherApi(androidClientSecretFile) : null;
//		if (true) {
//			test(publisher, "osm_live_subscription_annual_free_v2", args[1]);
//			return;
//		}
		Class.forName("org.postgresql.Driver");
		List<SubscriptionUpdateException> exceptionsUpdates = new ArrayList<UpdateSubscription.SubscriptionUpdateException>();
		Connection conn = DriverManager.getConnection(System.getenv("DB_CONN"),
				System.getenv("DB_USER"), System.getenv("DB_PWD"));
		for (SubscriptionType t : set) {
			new UpdateSubscription(publisher, t, revalidateinvalid).queryPurchases(conn, up, exceptionsUpdates);
		}
		if (exceptionsUpdates.size() > 0) {
			StringBuilder msg = new StringBuilder();
			for (SubscriptionUpdateException e : exceptionsUpdates) {
				msg.append(e.orderid + " " + e.getMessage()).append("\n");
			}
			throw new IllegalStateException(msg.toString());
		}
	}

	void queryPurchases(Connection conn, UpdateParams pms, List<SubscriptionUpdateException> exceptionsUpdates) throws SQLException {
		ResultSet rs = conn.createStatement().executeQuery(selQuery);
		updStat = conn.prepareStatement(updQuery);
		delStat = conn.prepareStatement(delQuery);
		updCheckStat = conn.prepareStatement(updCheckQuery);

		HuaweiIAPHelper huaweiIAPHelper = null;
		AmazonIAPHelper amazonIAPHelper = null;
		AndroidPublisher.Purchases purchases = publisher != null ? publisher.purchases() : null;
		ReceiptValidationHelper receiptValidationHelper = SubscriptionType.IOS == subType ? new ReceiptValidationHelper() : null;
		while (rs.next()) {
			String purchaseToken = rs.getString("purchaseToken");
			String prevpurchaseToken = rs.getString("prevvalidpurchasetoken");

			String sku = rs.getString("sku");
			String orderId = rs.getString("orderid");
			Timestamp checkTime = rs.getTimestamp("checktime");
			Timestamp regTime = rs.getTimestamp("timestamp");
			Timestamp startTime = rs.getTimestamp("starttime");
			Timestamp expireTime = rs.getTimestamp("expiretime");
			int introcycles = rs.getInt("introcycles");
			boolean valid = rs.getBoolean("valid");
			long currentTime = System.currentTimeMillis();
			SubscriptionType type = SubscriptionType.getSubType(purchaseToken, sku);
			if (this.subType != type) {
				continue;
			}
			long delayBetweenChecks = checkTime == null ? MINIMUM_WAIT_TO_REVALIDATE : (currentTime - checkTime.getTime());
			boolean forceCheckOrderId = !Algorithms.isEmpty(pms.orderId) && pms.orderId.equals(orderId);
			if (delayBetweenChecks < MINIMUM_WAIT_TO_REVALIDATE && !pms.verifyAll && !forceCheckOrderId) {
				// in case validate all (ignore minimum waiting time)
				continue;
			}
			
			// Don't validate FastSpring subscriptions if they are less than 15 minutes old
			if (this.subType == SubscriptionType.FASTSPRING && regTime != null 
					&& FastSpringHelper.isTooEarlyToValidate(regTime.getTime()) && !pms.verifyAll && !forceCheckOrderId) {
				long timeSincePurchase = currentTime - regTime.getTime();
				String hiddenOrderId = orderId != null ? (orderId.substring(0, Math.min(orderId.length(), 18)) + "...") : orderId;
				System.out.printf("Skipping FastSpring subscription validation - subscription is too recent (%d minutes old): %s - %s%n",
						timeSincePurchase / (60 * 1000), sku, hiddenOrderId);
				continue;
			}
			
			boolean activeNow = false;
			if (checkTime != null && startTime != null && expireTime != null && expireTime.getTime() >= currentTime) {
				activeNow = true;
			}
			// if it is not valid then it was requested to validate all
			if (activeNow && valid && delayBetweenChecks < MINIMUM_WAIT_TO_REVALIDATE_VALID) {
				continue;
			}
			String hiddenOrderId = orderId != null ? (orderId.substring(0, Math.min(orderId.length(), 18)) + "...")
					: orderId;
			System.out.println(String.format("Validate subscription (%s, %s): %s - %s (active=%s)", sku, hiddenOrderId,
					startTime == null ? "" : new Date(startTime.getTime()),
					expireTime == null ? "" : new Date(expireTime.getTime()), activeNow + ""));
			try {
				SubscriptionPurchase sub = null;
				if (subType == SubscriptionType.IOS) {
					sub = processIosSubscription(receiptValidationHelper, purchaseToken, sku, orderId,
							regTime, startTime, expireTime, currentTime, introcycles, pms);
				} else if (subType == SubscriptionType.HUAWEI) {
					if (huaweiIAPHelper == null) {
						huaweiIAPHelper = new HuaweiIAPHelper();
					}
					sub = processHuaweiSubscription(huaweiIAPHelper, purchaseToken, sku, orderId,
							regTime, startTime, expireTime, currentTime, pms);
				} else if (subType == SubscriptionType.AMAZON) {
					if (amazonIAPHelper == null) {
						amazonIAPHelper = new AmazonIAPHelper();
					}
					sub = processAmazonSubscription(amazonIAPHelper, purchaseToken, sku, orderId,
							regTime, startTime, expireTime, currentTime, pms);
				} else if (subType == SubscriptionType.ANDROID) {
					sub = processAndroidSubscription(purchases, purchaseToken, sku, orderId,
							regTime, startTime, expireTime, currentTime, pms);
				} else if (subType == SubscriptionType.FASTSPRING) {
					sub = processFastSpringSubscription(sku, orderId, startTime, expireTime, currentTime, pms);
				} else if (subType == SubscriptionType.PROMO) {
					processPromoSubscription(orderId, sku, expireTime, currentTime);
				}
				if (sub == null && prevpurchaseToken != null) {
					exceptionsUpdates.add(new SubscriptionUpdateException(orderId, "This situation need to be checked, we have prev valid purchase token but current token is not valid."));
				}
			} catch (SubscriptionUpdateException e) {
				exceptionsUpdates.add(e);
			}
		}
		if (deletions > 0) {
			delStat.executeBatch();
		}
		if (changes > 0) {
			updStat.executeBatch();
		}
		if (checkChanges > 0) {
			updCheckStat.executeBatch();
		}
		if (!conn.getAutoCommit()) {
			conn.commit();
		}
	}

	private void processPromoSubscription(String orderId, String sku, Timestamp expireTime, long currentTime) throws SQLException {
		if (expireTime != null && currentTime > expireTime.getTime()) {
			deleteSubscription(orderId, sku, currentTime, "promo expired", EXPIRED_STATE);
			return;
		}
		int ind = 1;
		updCheckStat.setTimestamp(ind++, new Timestamp(currentTime));
		updCheckStat.setString(ind++, orderId);
		updCheckStat.setString(ind, sku);
		updCheckStat.addBatch();
		checkChanges++;
		if (checkChanges > BATCH_SIZE) {
			updCheckStat.executeBatch();
			checkChanges = 0;
		}
	}

	private SubscriptionPurchase processIosSubscription(ReceiptValidationHelper receiptValidationHelper, String purchaseToken, String sku, String orderId,
			Timestamp regTime, Timestamp startTime, Timestamp expireTime, long currentTime, int prevIntroCycles,
			UpdateParams pms) throws SQLException, SubscriptionUpdateException {
		try {
			SubscriptionPurchase subscription = null;
			String reasonToDelete = null;
			String kind = "";
			ReceiptResult loadReceipt = receiptValidationHelper.loadReceipt(purchaseToken, false);
			if (loadReceipt.error == ReceiptValidationHelper.SANDBOX_ERROR_CODE_TEST) {
//				kind = "invalid";
//				reasonToDelete = "receipt from sandbox environment";
				loadReceipt = receiptValidationHelper.loadReceipt(purchaseToken, true);
			}
			if (loadReceipt.result) {
				JsonObject receiptObj = loadReceipt.response;
				if (pms.verbose) {
					System.out.println("Result: " + receiptObj.toString());
				}
				subscription = parseIosSubscription(sku, orderId, prevIntroCycles, receiptObj);
				if (subscription == null) {
					kind = "empty";
					reasonToDelete = "no purchases purchase format.";
				}
			} else if (loadReceipt.error == ReceiptValidationHelper.USER_GONE) {
				// sandbox: do not update anymore
				kind = "invalid";
				reasonToDelete = "user gone";
			}
			if (subscription != null) {
				updateSubscriptionDb(orderId, sku, startTime, expireTime, currentTime, subscription, pms);
			} else if (reasonToDelete != null) {
				deleteSubscription(orderId, sku, currentTime, reasonToDelete, kind);
			} else {
				if (pms.verbose && loadReceipt.error > 0 && loadReceipt.response != null) {
					System.out.println("Error status: " + loadReceipt.error + ", json:" + loadReceipt.response.toString());
				}
				System.err.println(String.format(
						"?? Error updating  sku %s and orderid %s (should be checked and fixed)!", sku, orderId));
			}
			return subscription;
		} catch (RuntimeException e) {
			LOGGER.warn(e.getMessage(), e);
			throw new SubscriptionUpdateException(orderId, String.format("??- Error updating  sku %s and orderid %s (should be checked and fixed)!!!: %s", sku,
					orderId, e.getMessage()));
		}
	}

	public static SubscriptionPurchase parseIosSubscription(String sku, String orderId, int prevIntroCycles,
			JsonObject receiptObj) {
		SubscriptionPurchase subscription = null;
		List<InAppReceipt> inAppReceipts = ReceiptValidationHelper.parseInAppReceipts(receiptObj);
		if (!inAppReceipts.isEmpty()) {
			Boolean autoRenewing = null;
			int introCycles = 0;
			long startDate = 0;
			long expiresDate = 0;
			String appstoreOrderId = null;
			for (InAppReceipt receipt : inAppReceipts) {
				// there could be multiple subscriptions for same purchaseToken !
				// i.e. 2020-04-01 -> 2021-04-01 + 2021-04-05 -> 2021-04-05
				if (sku.equals(receipt.getProductId()) && orderId.equals(receipt.getOrderId())) {
					appstoreOrderId = receipt.getOrderId();
					Map<String, String> fields = receipt.fields;
					// purchase_date_ms is purchase date of prolongation
					boolean introPeriod = "true".equals(fields.get("is_in_intro_offer_period"));
					long inAppStartDateMs = Long.parseLong(fields.get("purchase_date_ms"));
					long inAppExpiresDateMs = Long.parseLong(fields.get("expires_date_ms"));
					if (startDate == 0 || inAppStartDateMs < startDate) {
						startDate = inAppStartDateMs;
					}
					if (inAppExpiresDateMs > expiresDate) {
						autoRenewing = receipt.autoRenew;
						expiresDate = inAppExpiresDateMs;
					}
					if (introPeriod) {
						introCycles++;
					}
				}
			}
			if (expiresDate > 0) {
				IntroductoryPriceInfo ipo = null;
				introCycles = Math.max(prevIntroCycles, introCycles);
				if (introCycles > 0) {
					ipo = new IntroductoryPriceInfo();
					ipo.setIntroductoryPriceCycles(introCycles);
				}
				subscription = new SubscriptionPurchase()
						.setIntroductoryPriceInfo(ipo)
						.setStartTimeMillis(startDate)
						.setExpiryTimeMillis(expiresDate)
						.setAutoRenewing(autoRenewing)
						.setOrderId(appstoreOrderId);
				if (!Algorithms.objectEquals(subscription.getOrderId(), orderId)) {
					throw new IllegalStateException(
							String.format("Order id '%s' != '%s' don't match", orderId, subscription.getOrderId()));
				}
			}
		}
		return subscription;
	}

	private SubscriptionPurchase processHuaweiSubscription(HuaweiIAPHelper huaweiIAPHelper, String purchaseToken, String sku, String orderId,
			Timestamp regTime, Timestamp startTime, Timestamp expireTime, long currentTime, UpdateParams pms) throws SQLException, SubscriptionUpdateException {
		HuaweiSubscription subscription = null;
		String reason = "";
		String kind = null;
		try {
			// TODO continue
			// can't process null huawei
			if (orderId == null) {
				return null;
			}
			subscription = huaweiIAPHelper.getHuaweiSubscription(orderId, purchaseToken);
			if (pms.verbose) {
				System.out.println("Result: " + subscription.toString());
			}
		} catch (IOException e) {
			int errorCode = 0;
			if (e instanceof HuaweiJsonResponseException) {
				errorCode = ((HuaweiJsonResponseException) e).responseCode;
			}
			if (expireTime != null && currentTime - expireTime.getTime() > MAX_WAITING_TIME_TO_EXPIRE) {
				reason = String.format(" subscription expired more than %.1f days ago (%s)",
						(currentTime - expireTime.getTime()) / (DAY * 1.0d), e.getMessage());
				kind = EXPIRED_STATE;
			} else if (errorCode == HuaweiIAPHelper.RESPONSE_CODE_USER_ACCOUNT_ERROR ||
					errorCode == HuaweiIAPHelper.RESPONSE_CODE_USER_CONSUME_ERROR) {
				kind = "gone";
				reason = " user doesn't exist (" + e.getMessage() + ") ";
			} else {
				reason = " unknown reason (should be checked and fixed)! " + e.getMessage();
			}
		}
		SubscriptionPurchase subscriptionPurchase = null;
		if (subscription != null) {
			String appStoreOrderId = simplifyOrderId(subscription.subscriptionId);
			if (!Algorithms.objectEquals(appStoreOrderId, orderId)) {
				throw new IllegalStateException(
						String.format("Order id '%s' != '%s' don't match", orderId, appStoreOrderId));
			}
			subscriptionPurchase = new SubscriptionPurchase()
                    .setStartTimeMillis(startTime != null ? startTime.getTime() : subscription.purchaseTime)
					.setExpiryTimeMillis(subscription.expirationDate)
					.setAutoRenewing(subscription.autoRenewing)
					.setDeveloperPayload(subscription.developerPayload)
					.setPriceAmountMicros(subscription.price * 10000)
					.setPriceCurrencyCode(subscription.currency)
					.setOrderId(subscription.subscriptionId);
			if (!Algorithms.objectEquals(subscription.subscriptionId, orderId)) {
				throw new IllegalStateException(
						String.format("Order id '%s' != '%s' don't match", orderId, subscription.subscriptionId));
			}
			updateSubscriptionDb(orderId, sku, startTime, expireTime, currentTime, subscriptionPurchase, pms);
		} else if (kind != null) {
			deleteSubscription(orderId, sku, currentTime, reason, kind);
		} else {
			System.err.println(String.format("ERROR updating sku '%s' orderId '%s': %s", sku, orderId, reason));
			int ind = 1;
			updCheckStat.setTimestamp(ind++, new Timestamp(currentTime));
			updCheckStat.setString(ind++, orderId);
			updCheckStat.setString(ind++, sku);
			updCheckStat.addBatch();
			checkChanges++;
			if (checkChanges > BATCH_SIZE) {
				updCheckStat.executeBatch();
				checkChanges = 0;
			}
		}
		return subscriptionPurchase;
	}

	private SubscriptionPurchase processAmazonSubscription(AmazonIAPHelper amazonIAPHelper, String purchaseToken, String sku, String orderId,
			Timestamp regTime, Timestamp startTime, Timestamp expireTime, long currentTime, UpdateParams pms) throws SQLException, SubscriptionUpdateException {
		AmazonSubscription subscription = null;
		String reason = "";
		String kind = null;
		try {
			if (orderId == null) {
				return null;
			}
			subscription = amazonIAPHelper.getAmazonSubscription(orderId, purchaseToken);
			if (pms.verbose) {
				System.out.println("Result: " + subscription.toString());
			}
		} catch (IOException e) {
			int errorCode = 0;
			if (e instanceof AmazonIOException) {
				errorCode = ((AmazonIOException) e).responseCode;
			}
			if (expireTime != null && currentTime - expireTime.getTime() > MAX_WAITING_TIME_TO_EXPIRE) {
				reason = String.format(" subscription expired more than %.1f days ago (%s)",
						(currentTime - expireTime.getTime()) / (DAY * 1.0d), e.getMessage());
				kind = EXPIRED_STATE;
			} else if (errorCode == AmazonIAPHelper.RESPONSE_CODE_USER_ID_ERROR ||
					errorCode == AmazonIAPHelper.RESPONSE_CODE_TRANSACTION_ERROR) {
				kind = "gone";
				reason = " user doesn't exist (" + e.getMessage() + ") ";
			} else {
				reason = " unknown reason (should be checked and fixed)! " + e.getMessage();
			}
		}
		SubscriptionPurchase subscriptionPurchase = null;
		if (subscription != null) {
			String appStoreOrderId = simplifyOrderId(subscription.receiptId);
			if (!Algorithms.objectEquals(appStoreOrderId, orderId)) {
				throw new IllegalStateException(
						String.format("Order id '%s' != '%s' don't match", orderId, appStoreOrderId));
			}
			Long expiryTime = subscription.cancelDate != null ? subscription.cancelDate : subscription.renewalDate;
			subscriptionPurchase = new SubscriptionPurchase()
					.setStartTimeMillis(subscription.purchaseDate)
					.setExpiryTimeMillis(expiryTime)
					.setAutoRenewing(subscription.autoRenewing)
					.setOrderId(subscription.receiptId);
			if (!Algorithms.objectEquals(subscription.receiptId, orderId)) {
				throw new IllegalStateException(
						String.format("Order id '%s' != '%s' don't match", orderId, subscription.receiptId));
			}
			updateSubscriptionDb(orderId, sku, startTime, expireTime, currentTime, subscriptionPurchase, pms);
		} else if (kind != null) {
			deleteSubscription(orderId, sku, currentTime, reason, kind);
		} else {
			System.err.println(String.format("ERROR updating sku '%s' orderId '%s': %s", sku, orderId, reason));
			int ind = 1;
			updCheckStat.setTimestamp(ind++, new Timestamp(currentTime));
			updCheckStat.setString(ind++, orderId);
			updCheckStat.setString(ind++, sku);
			updCheckStat.addBatch();
			checkChanges++;
			if (checkChanges > BATCH_SIZE) {
				updCheckStat.executeBatch();
				checkChanges = 0;
			}
		}
		return subscriptionPurchase;
	}

	private SubscriptionPurchase processAndroidSubscription(AndroidPublisher.Purchases purchases, String purchaseToken, String sku, String orderId,
			Timestamp regTime, Timestamp startTime, Timestamp expireTime, long currentTime, UpdateParams pms) throws SQLException, SubscriptionUpdateException {
		SubscriptionPurchase subscription = null;
		String reason = "";
		String kind = null;
		try {
			if (sku.startsWith("osm_free") || sku.contains("_free_")) {
				subscription = purchases.subscriptions().get(GOOGLE_PACKAGE_NAME_FREE, sku, purchaseToken).execute();
			} else {
				subscription = purchases.subscriptions().get(GOOGLE_PACKAGE_NAME, sku, purchaseToken).execute();
			}
			if (pms.verbose) {
				System.out.println("Result: " + subscription.toPrettyString());
			}
		} catch (IOException e) {
			int errorCode = 0;
			if (e instanceof GoogleJsonResponseException) {
				errorCode = ((GoogleJsonResponseException) e).getStatusCode();
			}
			if (expireTime != null && currentTime - expireTime.getTime() > MAX_WAITING_TIME_TO_EXPIRE) {
				reason = String.format(" subscription expired more than %.1f days ago (%s)",
						(currentTime - expireTime.getTime()) / (DAY * 1.0d), e.getMessage());
				kind = EXPIRED_STATE;
			} else if (!purchaseToken.contains(".AO") || errorCode == 400) {
				reason = String.format(" subscription is invalid - possibly fraud %s, %s (%s)", orderId, purchaseToken, e.getMessage());
				if((currentTime - regTime.getTime()) > MAX_WAITING_TIME_TO_MAKE_INVALID) {
					kind = "invalid";
				}
			} else if (errorCode == 410) {
				kind = "gone";
				reason = " user doesn't exist (" + e.getMessage() + ") ";
			} else {
				reason = " unknown reason (should be checked and fixed)! " + e.getMessage();
			}
		}
		if (subscription != null) {
			String appStoreOrderId = simplifyOrderId(subscription.getOrderId());
			if (!Algorithms.objectEquals(appStoreOrderId, orderId)) {
				throw new IllegalStateException(
						String.format("Order id '%s' != '%s' don't match", orderId, appStoreOrderId));
			}
			updateSubscriptionDb(orderId, sku, startTime, expireTime, currentTime, subscription, pms);
		} else if (kind != null) {
			deleteSubscription(orderId, sku, currentTime, reason, kind);
		} else {
			System.err.println(String.format("ERROR updating sku '%s' orderId '%s': %s", sku, orderId, reason));
			int ind = 1;
			updCheckStat.setTimestamp(ind++, new Timestamp(currentTime));
			updCheckStat.setString(ind++, orderId);
			updCheckStat.setString(ind++, sku);
			updCheckStat.addBatch();
			checkChanges++;
			if (checkChanges > BATCH_SIZE) {
				updCheckStat.executeBatch();
				checkChanges = 0;
			}
		}
		return subscription;
	}

	private SubscriptionPurchase processFastSpringSubscription(String sku, String orderId, Timestamp startTime, Timestamp expireTime,
	                                                          long currentTime, UpdateParams pms) throws SQLException, SubscriptionUpdateException {
		SubscriptionPurchase subscription = null;
		String reason = "";
		String kind = null;

		try {
			FastSpringSubscription fsSub = FastSpringHelper.getSubscriptionByOrderIdAndSku(orderId, sku);
			if (fsSub == null) {
				reason = "FastSpring: subscription not found";
			} else if (!Boolean.TRUE.equals(fsSub.active)) {
				kind = EXPIRED_STATE;
				reason = "FastSpring: subscription not active";
			} else {
				subscription = new SubscriptionPurchase();
				subscription.setOrderId(fsSub.id);
				subscription.setStartTimeMillis(fsSub.begin);
				subscription.setExpiryTimeMillis(fsSub.nextChargeDate);
				subscription.setAutoRenewing(fsSub.autoRenew);
				subscription.setPriceAmountMicros(Math.round(fsSub.price * 1000000));
				subscription.setPriceCurrencyCode(fsSub.currency);

				if (pms.verbose) {
					LOGGER.info("Result: " + subscription.toPrettyString());
				}
			}
		} catch (Exception e) {
			if (expireTime != null && currentTime - expireTime.getTime() > MAX_WAITING_TIME_TO_EXPIRE) {
				reason = String.format(" subscription expired more than %.1f days ago (%s)",
						(currentTime - expireTime.getTime()) / (DAY * 1.0d), e.getMessage());
				kind = EXPIRED_STATE;
			} else {
				reason = "FastSpring error: " + e.getMessage();
			}
		}

		if (subscription != null) {
			updateSubscriptionDb(orderId, sku, startTime, expireTime, currentTime, subscription, pms);
			changes++;
		} else if (kind != null) {
			deleteSubscription(orderId, sku, currentTime, reason, kind);
		} else {
			LOGGER.error(String.format("ERROR updating sku '%s' orderId '%s': %s", sku, orderId, reason));
			int ind = 1;
			updCheckStat.setTimestamp(ind++, new Timestamp(currentTime));
			updCheckStat.setString(ind++, orderId);
			updCheckStat.setString(ind++, sku);
			updCheckStat.addBatch();
			checkChanges++;
			if (checkChanges > BATCH_SIZE) {
				updCheckStat.executeBatch();
				checkChanges = 0;
			}
		}
		return subscription;
	}

	private String simplifyOrderId(String orderId) {
		int i = orderId.indexOf("..");
		if (i >= 0) {
			return orderId.substring(0, i);
		}
		return orderId;
	}

	private void deleteSubscription(String orderId, String sku, long tm, String reason, String kind) throws SQLException {
		delStat.setString(1, kind);
		delStat.setTimestamp(2, new Timestamp(tm));
		delStat.setString(3, orderId);
		delStat.setString(4, sku);
		delStat.addBatch();
		deletions++;
		System.out.println(String.format(
				"!! Deleting subscription: sku=%s, orderId=%s. Reason: %s ", sku, orderId, reason));
		if (deletions > BATCH_SIZE) {
			delStat.executeBatch();
			deletions = 0;
		}
	}

	private boolean updateSubscriptionDb(String orderId, String sku, Timestamp startTime, Timestamp expireTime,
									  long tm, SubscriptionPurchase subscription, UpdateParams pms) throws SQLException, SubscriptionUpdateException {
		boolean updated = false;
		int ind = 1;
		updStat.setTimestamp(ind++, new Timestamp(tm));
		if (subscription.getStartTimeMillis() != null) {
			int maxDays = 40;
			if (startTime != null && Math.abs(startTime.getTime() - subscription.getStartTimeMillis()) > maxDays * DAY && startTime.getTime() > 100000 * 1000L) {
				if (!pms.forceUpdate) {
					throw new SubscriptionUpdateException(orderId, String.format(
							"ERROR: Start timestamp changed more than %d days '%s' (db) != '%s' (appstore) '%s' %s",
							maxDays, new Date(startTime.getTime()), new Date(subscription.getStartTimeMillis()), orderId, sku));
				} else {
					System.err.println(String.format("ERROR: Start timestamp changed more than 14 days '%s' (db) != '%s' (appstore) '%s' %s",
							new Date(startTime.getTime()),
							new Date(subscription.getStartTimeMillis()), orderId, sku));
				}
			}
			updStat.setTimestamp(ind++, new Timestamp(subscription.getStartTimeMillis()));
			updated = true;
		} else {
			updStat.setTimestamp(ind++, startTime);
		}
		if (subscription.getExpiryTimeMillis() != null) {
			if (expireTime == null || Math.abs(expireTime.getTime() - subscription.getExpiryTimeMillis()) > 10 * 1000) {
				System.out.println(String.format("Expire timestamp changed %s != %s for '%s' %s",
						expireTime == null ? "" : new Date(expireTime.getTime()),
						new Date(subscription.getExpiryTimeMillis()), orderId, sku));
			}
			updStat.setTimestamp(ind++, new Timestamp(subscription.getExpiryTimeMillis()));
			updated = true;
		} else {
			updStat.setTimestamp(ind++, expireTime);
		}
		if (subscription.getAutoRenewing() == null) {
			updStat.setNull(ind++, Types.BOOLEAN);
		} else {
			updStat.setBoolean(ind++, subscription.getAutoRenewing());
		}
		if (subType == SubscriptionType.IOS) {
			IntroductoryPriceInfo info = subscription.getIntroductoryPriceInfo();
			if (info != null) {
				updStat.setInt(ind++, (int) info.getIntroductoryPriceCycles());
			} else {
				updStat.setNull(ind++, Types.INTEGER);
			}
		} else if (subType == SubscriptionType.HUAWEI) {
			updStat.setString(ind++, subscription.getDeveloperPayload());
			updStat.setInt(ind++, (int) (subscription.getPriceAmountMicros() / 1000l));
			updStat.setString(ind++, subscription.getPriceCurrencyCode());
		} else if (subType == SubscriptionType.AMAZON) {
			// none
		} else if (subType == SubscriptionType.FASTSPRING) {
			updStat.setInt(ind++, (int) (subscription.getPriceAmountMicros() / 1000l));
			updStat.setString(ind++, subscription.getPriceCurrencyCode());
		} else {
			if (subscription.getPaymentState() == null) {
				updStat.setNull(ind++, Types.INTEGER);
			} else {
				updStat.setInt(ind++, subscription.getPaymentState());
			}
			updStat.setString(ind++, subscription.getDeveloperPayload());
			updStat.setInt(ind++, (int) (subscription.getPriceAmountMicros() / 1000l));
			updStat.setString(ind++, subscription.getPriceCurrencyCode());
			IntroductoryPriceInfo info = subscription.getIntroductoryPriceInfo();
			if (info != null) {
				updStat.setInt(ind++, (int) (info.getIntroductoryPriceAmountMicros() / 1000l));
				updStat.setString(ind++, info.getIntroductoryPriceCurrencyCode());
				updStat.setInt(ind++, (int) info.getIntroductoryPriceCycles());
				updStat.setString(ind++, info.getIntroductoryPricePeriod());
			} else {
				updStat.setNull(ind++, Types.INTEGER);
				updStat.setNull(ind++, Types.VARCHAR);
				updStat.setNull(ind++, Types.INTEGER);
				updStat.setNull(ind++, Types.VARCHAR);
			}
		}

		if (subscription.getExpiryTimeMillis() != null) {
			boolean expired = tm - subscription.getExpiryTimeMillis() > MAX_WAITING_TIME_TO_EXPIRE;
			updStat.setBoolean(ind++, !expired);
			if (expired) {
				updated = true;
				updStat.setString(ind++, EXPIRED_STATE);
			} else {
				updStat.setNull(ind++, Types.VARCHAR);
			}
		} else {
			updStat.setBoolean(ind++, true);
			updStat.setNull(ind++, Types.VARCHAR);
		}

		updStat.setString(ind++, orderId);
		updStat.setString(ind, sku);
		System.out.println(String.format("%s %s start %s expire %s",
				updated ? "Updates " : "No changes ", sku,
				startTime == null ? "" : new Date(startTime.getTime()),
				expireTime == null ? "" : new Date(expireTime.getTime())
		));
		if (updated) {
			updStat.addBatch();
			changes++;
			if (changes > BATCH_SIZE) {
				updStat.executeBatch();
				changes = 0;
			}
		}
		return updated;
	}

	private static HttpRequestInitializer setHttpTimeout(final HttpRequestInitializer requestInitializer) {
		  return new HttpRequestInitializer() {
		    @Override
		    public void initialize(HttpRequest httpRequest) throws IOException {
		      requestInitializer.initialize(httpRequest);
		      // 12 min
		      httpRequest.setConnectTimeout(12 * 60000);
		      httpRequest.setReadTimeout(12 * 60000);
		    }
		  };
	}

	public static AndroidPublisher getPublisherApi(String file) throws JSONException, IOException, GeneralSecurityException {
		List<String> scopes = new ArrayList<String>();
		scopes.add("https://www.googleapis.com/auth/androidpublisher");
	    File dataStoreDir = new File(new File(file).getParentFile(), ".credentials");
	    JacksonFactory jsonFactory = new com.google.api.client.json.jackson2.JacksonFactory();

	    GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(jsonFactory, new InputStreamReader(new FileInputStream(file)));
	    NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

		// Build flow and trigger user authorization request.
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
				httpTransport, jsonFactory, clientSecrets, scopes)
						.setDataStoreFactory(new FileDataStoreFactory(dataStoreDir))
						.setAccessType("offline")
						.build();
		Builder bld = new LocalServerReceiver.Builder();
		bld.setPort(5000);
		// it only works with localhost ! with other hosts gives incorrect redirect uri (looks like it's not supported for service accounts)
		// bld.setHost(serverPublicUrl);
		Credential credential = new AuthorizationCodeInstalledApp(flow, bld.build()).authorize("user");
		System.out.println("Credentials saved to " + dataStoreDir.getAbsolutePath());
		AndroidPublisher publisher = new AndroidPublisher.Builder(httpTransport, jsonFactory, setHttpTimeout(credential))
				.setApplicationName(GOOGLE_PRODUCT_NAME).build();

		return publisher;
	}


	protected static void test(AndroidPublisher publisher, String subscriptionId, String purchaseToken) {
		try {
			com.google.api.services.androidpublisher.AndroidPublisher.Inappproducts.List lst = publisher.inappproducts().list(GOOGLE_PACKAGE_NAME_FREE);
			InappproductsListResponse response = lst.execute();
			for (InAppProduct p : response.getInappproduct()) {
				System.out.println("SKU=" + p.getSku() +
						" type=" + p.getPurchaseType() +
						" LNG=" + p.getDefaultLanguage() +
						//" P="+p.getPrices()+
						" Period=" + p.getSubscriptionPeriod() + " Status=" + p.getStatus());
			}
			if(subscriptionId.length() > 0 && purchaseToken.length() > 0) {
				AndroidPublisher.Purchases purchases = publisher.purchases();
				SubscriptionPurchase subscription = purchases.subscriptions().get(GOOGLE_PACKAGE_NAME_FREE, subscriptionId, purchaseToken).execute();
				System.out.println(subscription.getUnknownKeys());
				System.out.println(subscription.getAutoRenewing());
				System.out.println(subscription.getKind());
				System.out.println(new Date(subscription.getExpiryTimeMillis()));
				System.out.println(new Date(subscription.getStartTimeMillis()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected static class SubscriptionUpdateException extends Exception {

		private static final long serialVersionUID = -7058574093912760811L;
		private String orderid;

		public SubscriptionUpdateException(String orderid, String message) {
			super(message);
			this.orderid = orderid;
		}

		public String getOrderid() {
			return orderid;
		}
	}





}
