package net.osmand.live.subscriptions;

import java.io.BufferedReader;
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
import java.util.List;
import java.util.Map;

import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver.Builder;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.model.InAppProduct;
import com.google.api.services.androidpublisher.model.InappproductsListResponse;
import com.google.api.services.androidpublisher.model.SubscriptionPurchase;
import com.google.gson.JsonObject;

import net.osmand.live.subscriptions.ReceiptValidationHelper.InAppReceipt;
import net.osmand.util.Algorithms;


public class UpdateSubscription {


	private static final String INVALID_PURCHASE = "invalid";
	private static String PATH_TO_KEY = "";
	// init one time
	private static String GOOGLE_CLIENT_CODE = "";
	private static String GOOGLE_CLIENT_ID = "";
	private static String GOOGLE_CLIENT_SECRET = "";
	private static String GOOGLE_REDIRECT_URI = "";
	// https://accounts.google.com/o/oauth2/token
	public final static String GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token";
	public final static String GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/auth";
	public final static String GOOGLE_ACCESS_TOKEN_URL = "https://www.googleapis.com/oauth2/v4/token";
	
	private static String TOKEN = "";


	private static final String GOOGLE_PRODUCT_NAME = "OsmAnd+";
	private static final String GOOGLE_PRODUCT_NAME_FREE = "OsmAnd";

	private static final String GOOGLE_PACKAGE_NAME = "net.osmand.plus";
	private static final String GOOGLE_PACKAGE_NAME_FREE = "net.osmand";
	private static final int BATCH_SIZE = 200;
	private static final long DAY = 1000l * 60 * 60 * 24;
	private static final long HOUR = 1000l * 60 * 60;

	private static HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static JsonFactory JSON_FACTORY = new com.google.api.client.json.jackson2.JacksonFactory();

	int changes = 0;
	int deletions = 0;
	protected String selQuery;
	protected String updQuery;
	protected String delQuery;
//	protected String updCheckQuery;
	protected PreparedStatement updStat;
	protected PreparedStatement delStat;
//	protected PreparedStatement updCheckStat;
	protected boolean ios;

	public static void main(String[] args) throws JSONException, IOException, SQLException, ClassNotFoundException, GeneralSecurityException {
		AndroidPublisher publisher = getPublisherApi(args[0]);
//		if(true ){
//			test(publisher, "","");
//			return;
//		}

		Class.forName("org.postgresql.Driver");
		Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5433/changeset",
				System.getenv("DB_USER"), System.getenv("DB_PWD"));
		boolean verifyAll = false;
		for (int i = 1; i < args.length; i++) {
			if ("-verifyall".equals(args[i])) {
				verifyAll = true;
			}
		}
		new UpdateAndroidSubscription(publisher).queryPurchases(conn, verifyAll);
		new UpdateIosSubscription().queryPurchases(conn, verifyAll);
	}

	public UpdateSubscription() {
		delQuery = "UPDATE supporters_device_sub SET valid = false, kind = ?, checktime = ? " +
				"WHERE userid = ? and purchaseToken = ? and sku = ?";
//			updCheckQuery = "UPDATE supporters_device_sub SET checktime = ? " +
//					"WHERE userid = ? and purchaseToken = ? and sku = ?";
	}

	private static class UpdateAndroidSubscription extends UpdateSubscription {
		private AndroidPublisher publisher;

		UpdateAndroidSubscription(AndroidPublisher publisher) {
			super();
			this.publisher = publisher;
			this.ios = false;
			selQuery = "SELECT userid, sku, purchaseToken, checktime, starttime, expiretime, valid " +
					"FROM supporters_device_sub S where (valid is null or valid=true) ";
			updQuery = "UPDATE supporters_device_sub SET " +
					"checktime = ?, starttime = ?, expiretime = ?, autorenewing = ?, kind = ?, orderid = ?, payload = ?, valid = ? " +
					"WHERE userid = ? and purchaseToken = ? and sku = ?";
		}

		@Override
		void queryPurchases(Connection conn, boolean verifyAll) throws SQLException {
			queryPurchasesImpl(publisher, conn, verifyAll);
		}
	}

	private static class UpdateIosSubscription extends UpdateSubscription {
		UpdateIosSubscription() {
			super();
			this.ios = true;
			selQuery = "SELECT userid, sku, purchaseToken, payload, checktime, starttime, expiretime, valid " +
					"FROM supporters_device_sub S where (valid is null or valid=true) ";
			updQuery = "UPDATE supporters_device_sub SET " +
					"checktime = ?, starttime = ?, expiretime = ?, valid = ? " +
					"WHERE userid = ? and purchaseToken = ? and sku = ?";
		}

		@Override
		void queryPurchases(Connection conn, boolean verifyAll) throws SQLException {
			queryPurchasesImpl(null, conn, verifyAll);
		}
	}

	void queryPurchases(Connection conn, boolean verifyAll) throws SQLException {
		// non implemented
	}

	void queryPurchasesImpl(AndroidPublisher publisher, Connection conn, boolean verifyAll) throws SQLException {
		ResultSet rs = conn.createStatement().executeQuery(selQuery);
		updStat = conn.prepareStatement(updQuery);
		delStat = conn.prepareStatement(delQuery);
//		updCheckStat = conn.prepareStatement(updCheckQuery);

		AndroidPublisher.Purchases purchases = publisher != null ? publisher.purchases() : null;
		ReceiptValidationHelper receiptValidationHelper = this.ios ? new ReceiptValidationHelper() : null;

		while (rs.next()) {
			long userid = rs.getLong("userid");
			String pt = rs.getString("purchaseToken");
			String sku = rs.getString("sku");
			String payload = this.ios ? rs.getString("payload") : null;
			Timestamp checkTime = rs.getTimestamp("checktime");
			Timestamp startTime = rs.getTimestamp("starttime");
			Timestamp expireTime = rs.getTimestamp("expiretime");
			boolean valid = rs.getBoolean("valid");
			long tm = System.currentTimeMillis();
			boolean ios = sku.startsWith("net.osmand.maps.subscription.");
			if ((this.ios && !ios) || (!this.ios && ios)) {
				continue;
			}

			long checkDiff = checkTime == null ? tm : (tm - checkTime.getTime());
			// Basically validate non-valid everytime and valid not often than once per 24 hours
			if (checkDiff < HOUR || (valid && checkDiff < DAY)) {
//				if (verifyAll) {
//					System.out.println(String.format("Skip userid=%d, sku=%s - recently checked %.1f days", userid,
//							sku, (tm - checkTime.getTime()) / (DAY * 1.0)));
//				}
				continue;
			}

			boolean activeNow = false;
			if (checkTime != null && startTime != null && expireTime != null) {
				if (expireTime.getTime() >= tm) {
					activeNow = true;
				}
			}
			// skip all active and valid if it was validated less than 5 days ago
			if (activeNow && valid) {
				if(checkDiff < 5 * DAY || !verifyAll) {
					if(verifyAll) {
						System.out.println(String.format("Skip userid=%d, sku=%s - subscribtion is active", userid, sku));
					}
					continue;
				}
			}

			if (this.ios) {
				processIosSubscription(receiptValidationHelper, userid, pt, sku, payload, startTime, expireTime, tm);
			} else {
				processAndroidSubscription(purchases, userid, pt, sku, startTime, expireTime, tm);
			}
		}
		if (deletions > 0) {
			delStat.executeBatch();
		}
		if (changes > 0) {
			updStat.executeBatch();
		}
		if (!conn.getAutoCommit()) {
			conn.commit();
		}
	}

	private void processIosSubscription(ReceiptValidationHelper receiptValidationHelper, long userid, String pt, String sku, String payload, Timestamp startTime, Timestamp expireTime, long tm) throws SQLException {
		try {
			String reason = null;
			String kind = "";

			Map<String, Object> map = receiptValidationHelper.loadReceipt(payload);
			Object result = map.get("result");
			Object error = map.get("error");
			if (result.equals(true)) {
				JsonObject receiptObj = (JsonObject) map.get("response");
				if (receiptObj != null) {
					Map<String, InAppReceipt> inAppReceipts = receiptValidationHelper.loadInAppReceipts(receiptObj);
					if (inAppReceipts != null) {
						if (inAppReceipts.size() == 0) {
							kind = "gone";
							reason = "subscription expired.";
						} else {
							InAppReceipt foundReceipt = null;
							for (InAppReceipt receipt : inAppReceipts.values()) {
								if (sku.equals(receipt.getProductId())) {
									foundReceipt = receipt;
									break;
								}
							}
							if (foundReceipt != null) {
								Map<String, String> fields = foundReceipt.fields;
								String purchaseDateStr = fields.get("original_purchase_date_ms");
								String expiresDateStr = fields.get("expires_date_ms");
								if (!Algorithms.isEmpty(expiresDateStr)) {
									try {
										long purchaseDateMs = Long.parseLong(purchaseDateStr);
										long expiresDateMs = Long.parseLong(expiresDateStr);
										SubscriptionPurchase subscription = new SubscriptionPurchase()
												.setStartTimeMillis(purchaseDateMs)
												.setExpiryTimeMillis(expiresDateMs);

										updateSubscriptionDb(userid, pt, sku, startTime, expireTime, tm, subscription);

										if (tm - expiresDateMs > 15 * DAY) {
											kind = "gone";
											reason = String.format("subscription expired more than %.1f days ago",
													(tm - expiresDateMs) / (DAY * 1.0d));
										}
									} catch (NumberFormatException e) {
										e.printStackTrace();
										kind = "gone";
										reason = "subscription expired.";
									}
								}
							} else {
								kind = "gone";
								reason = "subscription expired.";
							}
						}
					}
				}
			} else if (error.equals(ReceiptValidationHelper.SANDBOX_ERROR_CODE)) {
				// sandbox: do not update anymore
				kind = "invalid";
				reason = "receipt from sandbox environment";
			}
			if (reason != null) {
				deleteSubscription(userid, pt, sku, tm, reason, kind);
			}
		} catch (Exception e) {
			System.err.println(String.format("?? Error updating userid %s and sku %s: %s", userid, sku, e.getMessage()));
		}
	}

	private void processAndroidSubscription(AndroidPublisher.Purchases purchases, long userid, String pt, String sku, Timestamp startTime, Timestamp expireTime, long tm) throws SQLException {
		SubscriptionPurchase subscription;
		try {
			if (sku.startsWith("osm_free") || sku.contains("_free_")) {
				subscription = purchases.subscriptions().get(GOOGLE_PACKAGE_NAME_FREE, sku, pt).execute();
			} else {
				subscription = purchases.subscriptions().get(GOOGLE_PACKAGE_NAME, sku, pt).execute();
			}
			updateSubscriptionDb(userid, pt, sku, startTime, expireTime, tm, subscription);
		} catch (IOException e) {
			boolean gone = false;
			if (e instanceof GoogleJsonResponseException) {
				gone = ((GoogleJsonResponseException) e).getStatusCode() == 410;
			}

			String reason = null;
			String kind = "";
			if (!pt.contains(".AO")) {
				reason = "invalid purchase token " + e.getMessage();
				kind = "invalid";
			} else if (gone) {
				kind = "gone";
				if (expireTime == null) {
					reason = "subscription expired.";
				} else if (tm - expireTime.getTime() > 15 * DAY) {
					reason = String.format("subscription expired more than %.1f days ago",
							(tm - expireTime.getTime()) / (DAY * 1.0d));
				}

			}
			if (reason != null) {
				deleteSubscription(userid, pt, sku, tm, reason, kind);
			} else {
				System.err.println(String.format("?? Error updating userid %s and sku %s: %s", userid, sku, e.getMessage()));
			}
		}
	}

	private void deleteSubscription(long userid, String pt, String sku, long tm, String reason, String kind) throws SQLException {
		delStat.setString(1, kind);
		delStat.setTimestamp(2, new Timestamp(tm));
		delStat.setLong(3, userid);
		delStat.setString(4, pt);
		delStat.setString(5, sku);
		delStat.addBatch();
		deletions++;
		System.out.println(String.format(
				"!! Clearing possible invalid subscription: userid=%s, sku=%s. Reason: %s ", userid, sku, reason));
		if (deletions > BATCH_SIZE) {
			delStat.executeUpdate();
			deletions = 0;
		}
	}

	private void updateSubscriptionDb(long userid, String pt, String sku, Timestamp startTime, Timestamp expireTime,
									  long tm, SubscriptionPurchase subscription) throws SQLException {
		boolean updated = false;
		int ind = 1;
		updStat.setTimestamp(ind++, new Timestamp(tm));
		if (subscription.getStartTimeMillis() != null) {
			if (startTime != null && Math.abs(startTime.getTime() - subscription.getStartTimeMillis()) > 5 * 60 * 60 * 1000 &&
					startTime.getTime() > 100000 * 1000L) {
				throw new IllegalArgumentException(String.format("Start timestamp changed %s != %s %s %s",
						new Date(startTime.getTime()),
						new Date(subscription.getStartTimeMillis()), userid, sku));
			}
			updStat.setTimestamp(ind++, new Timestamp(subscription.getStartTimeMillis()));
			updated = true;
		} else {
			updStat.setTimestamp(ind++, startTime);
		}
		if (subscription.getExpiryTimeMillis() != null) {
			if (expireTime == null || Math.abs(expireTime.getTime() - subscription.getExpiryTimeMillis()) > 10 * 1000) {
				System.out.println(String.format("Expire timestamp changed %s != %s for %s %s",
						expireTime == null ? "" : new Date(expireTime.getTime()),
						new Date(subscription.getExpiryTimeMillis()), userid, sku));
			}
			updStat.setTimestamp(ind++, new Timestamp(subscription.getExpiryTimeMillis()));
			updated = true;
		} else {
			updStat.setTimestamp(ind++, expireTime);
		}
		if (!ios) {
			if (subscription.getAutoRenewing() == null) {
				updStat.setNull(ind++, Types.BOOLEAN);
			} else {
				updStat.setBoolean(ind++, subscription.getAutoRenewing());
			}
			updStat.setString(ind++, subscription.getKind());
			updStat.setString(ind++, subscription.getOrderId());
			updStat.setString(ind++, subscription.getDeveloperPayload());
		}
		updStat.setBoolean(ind++, true);
		updStat.setLong(ind++, userid);
		updStat.setString(ind++, pt);
		updStat.setString(ind, sku);
		System.out.println(String.format("%s for %s %s start %s expire %s",
				updated ? "Updates " : "No changes ",
				userid, sku,
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
	}


	
	private static AndroidPublisher getPublisherApi(String file) throws JSONException, IOException, GeneralSecurityException {
//		Properties properties = new Properties();
//		properties.load(new FileInputStream(file));
//		GOOGLE_CLIENT_CODE = properties.getProperty("GOOGLE_CLIENT_CODE");
//		GOOGLE_CLIENT_ID = properties.getProperty("GOOGLE_CLIENT_ID");
//		GOOGLE_CLIENT_SECRET = properties.getProperty("GOOGLE_CLIENT_SECRET");
//		GOOGLE_REDIRECT_URI = properties.getProperty("GOOGLE_REDIRECT_URI");
//		TOKEN = properties.getProperty("TOKEN");
//		generateAuthUrl();
//		String token = getRefreshToken();
//		String accessToken = getAccessToken(token);
//		TokenResponse tokenResponse = new TokenResponse();
//
//		System.out.println("refresh token=" + token);
//		System.out.println("access token=" + accessToken);
//
//		tokenResponse.setAccessToken(accessToken);
//		tokenResponse.setRefreshToken(token);
//		tokenResponse.setExpiresInSeconds(3600L);
//		tokenResponse.setScope("https://www.googleapis.com/auth/androidpublisher");
//		tokenResponse.setTokenType("Bearer");
//		HttpRequestInitializer credential = new GoogleCredential.Builder().setTransport(httpTransport)
//		.setJsonFactory(jsonFactory).setClientSecrets(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET).build()
//		.setFromTokenResponse(tokenResponse);

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
		if(System.getenv("HOSTNAME") != null) {
			bld.setHost(System.getenv("HOSTNAME"));
		}
		Credential credential = new AuthorizationCodeInstalledApp(flow, bld.build()).authorize("user");
		System.out.println("Credentials saved to " + dataStoreDir.getAbsolutePath());		
		

//

		AndroidPublisher publisher = new AndroidPublisher.Builder(httpTransport, jsonFactory, credential)
				.setApplicationName(GOOGLE_PRODUCT_NAME).build();

		return publisher;
	}


	private static void test(AndroidPublisher publisher, String subscriptionId, String purchaseToken) {
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
			AndroidPublisher.Purchases purchases = publisher.purchases();
			SubscriptionPurchase subscription = purchases.subscriptions().get(GOOGLE_PACKAGE_NAME_FREE, subscriptionId, purchaseToken).execute();
			System.out.println(subscription.getUnknownKeys());
			System.out.println(subscription.getAutoRenewing());
			System.out.println(subscription.getKind());
			System.out.println(new Date(subscription.getExpiryTimeMillis()));
			System.out.println(new Date(subscription.getStartTimeMillis()));
//			return subscription.getExpiryTimeMillis();
//			return subscripcion.getValidUntilTimestampMsec();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static String getAccessToken(String refreshToken) throws JSONException {
		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost(GOOGLE_ACCESS_TOKEN_URL);
		try {
			List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>(4);
			nameValuePairs.add(new BasicNameValuePair("grant_type", "refresh_token"));
			nameValuePairs.add(new BasicNameValuePair("client_id", GOOGLE_CLIENT_ID));
			nameValuePairs.add(new BasicNameValuePair("client_secret", GOOGLE_CLIENT_SECRET));
			nameValuePairs.add(new BasicNameValuePair("refresh_token", refreshToken));
			post.setEntity(new UrlEncodedFormEntity(nameValuePairs));

			org.apache.http.HttpResponse response = client.execute(post);
			BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			StringBuffer buffer = new StringBuffer();
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				buffer.append(line);
			}

			JSONObject json = new JSONObject(buffer.toString());
			String accessToken = json.getString("access_token");
			return accessToken;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	private static String generateAuthUrl() throws JSONException {
		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost(GOOGLE_AUTH_URL);
		try {
			List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>(4);
			nameValuePairs.add(new BasicNameValuePair("redirect_uri", GOOGLE_REDIRECT_URI));
			nameValuePairs.add(new BasicNameValuePair("client_id", GOOGLE_CLIENT_ID));
			nameValuePairs.add(new BasicNameValuePair("access_type", "offline"));
			nameValuePairs.add(new BasicNameValuePair("response_type", "code"));
			post.setEntity(new UrlEncodedFormEntity(nameValuePairs));

			org.apache.http.HttpResponse response = client.execute(post);
			BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			StringBuffer buffer = new StringBuffer();
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				buffer.append(line);
			}
			System.out.println(buffer);
			throw new UnsupportedOperationException("Follow url to active code " + buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static String getRefreshToken() {
		HttpClient client = new DefaultHttpClient();
									
		HttpPost post = new HttpPost(GOOGLE_TOKEN_URL);
		try {
			List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>(5);
			nameValuePairs.add(new BasicNameValuePair("grant_type", "authorization_code"));
			nameValuePairs.add(new BasicNameValuePair("client_id", GOOGLE_CLIENT_ID));
			nameValuePairs.add(new BasicNameValuePair("client_secret", GOOGLE_CLIENT_SECRET));
			nameValuePairs.add(new BasicNameValuePair("code", GOOGLE_CLIENT_CODE));
			nameValuePairs.add(new BasicNameValuePair("redirect_uri", GOOGLE_REDIRECT_URI));
			post.setEntity(new UrlEncodedFormEntity(nameValuePairs));

			org.apache.http.HttpResponse response = client.execute(post);
			BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			StringBuffer buffer = new StringBuffer();
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				buffer.append(line);
			}
			System.out.println(buffer.toString());
			JSONObject json = new JSONObject(buffer.toString());
			String refreshToken = json.getString("refresh_token");
			return refreshToken;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

}
