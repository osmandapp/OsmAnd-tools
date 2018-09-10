package net.osmand.live.subscriptions;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.Properties;

import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.model.InAppProduct;
import com.google.api.services.androidpublisher.model.InappproductsListResponse;
import com.google.api.services.androidpublisher.model.SubscriptionPurchase;


public class UpdateSubscription {


	private static final String INVALID_PURCHASE = "invalid";
	private static String PATH_TO_KEY = "";
	// init one time
	private static String GOOGLE_CLIENT_CODE="";
	private static String GOOGLE_CLIENT_ID = "";
	private static String GOOGLE_CLIENT_SECRET = "";
	private static String GOOGLE_REDIRECT_URI = "";
	private static String TOKEN = "";
	
	
	private static final String GOOGLE_PRODUCT_NAME = "OsmAnd+";
	private static final String GOOGLE_PRODUCT_NAME_FREE = "OsmAnd";
	
	private static final String GOOGLE_PACKAGE_NAME = "net.osmand.plus";
	private static final String GOOGLE_PACKAGE_NAME_FREE = "net.osmand";
	private static final int BATCH_SIZE = 200;
	private static final long DAY = 1000l * 60 * 60 * 24;
	
	private static HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static JsonFactory JSON_FACTORY = new com.google.api.client.json.jackson2.JacksonFactory();
	
	int changes = 0;
	int deletions = 0;
	private PreparedStatement updSubscrStat;
	private PreparedStatement delStat;
	private PreparedStatement updCheckStat;

	public static void main(String[] args) throws JSONException, IOException, SQLException, ClassNotFoundException {
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
		UpdateSubscription upd = new UpdateSubscription();
		upd.queryPurchases(publisher, conn, verifyAll);
	}
	
	private void queryPurchases(AndroidPublisher publisher, Connection conn, boolean verifyAll) throws SQLException {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT userid, sku, purchaseToken, checktime, starttime, expiretime, valid "
				+ "FROM supporters_device_sub S where (valid is null or valid=true) ");
		updSubscrStat = conn.prepareStatement("UPDATE supporters_device_sub SET "
				+ "checktime = ?, starttime = ?, expiretime = ?, autorenewing = ?, kind = ?, valid = ? " +
				  " WHERE userid = ? and purchaseToken = ? and sku = ?");
		
		delStat = conn.prepareStatement("UPDATE supporters_device_sub SET valid = false, kind = ?, checktime = ?" +
							" WHERE userid = ? and purchaseToken = ? and sku = ?");
		updCheckStat = conn.prepareStatement("UPDATE supporters_device_sub SET checktime = ? " +
							" WHERE userid = ? and purchaseToken = ? and sku = ?");

		AndroidPublisher.Purchases purchases = publisher.purchases();
		
		while (rs.next()) {
			String userid = rs.getString("userid");
			String pt = rs.getString("purchaseToken");
			String sku = rs.getString("sku");
			Timestamp checkTime = rs.getTimestamp("checktime");
			Timestamp startTime = rs.getTimestamp("starttime");
			Timestamp expireTime = rs.getTimestamp("expiretime");
			boolean vald = rs.getBoolean("valid");
			long tm = System.currentTimeMillis();
			if (checkTime != null && (tm - checkTime.getTime()) < DAY & vald) {
				if (verifyAll) {
					System.out.println(String.format("Skip userid=%s, sku=%s - recently checked %.1f days", userid,
							sku, (tm - checkTime.getTime()) / (DAY * 1.0)));
				}
				continue;
			}
			
			boolean activeNow = false;
			if(checkTime != null && startTime != null && expireTime != null){
				if(expireTime.getTime() >= tm) {
					activeNow = true;
				}
			}
			if(activeNow && !verifyAll && vald) {
				System.out.println(String.format("Skip userid=%s, sku=%s - subscribtion is active", userid, sku));
				continue;
			}
			
			SubscriptionPurchase subscription;
			try {
				if(sku.startsWith("osm_free")) {
					subscription = purchases.subscriptions().get(GOOGLE_PACKAGE_NAME_FREE, sku, pt).execute();
				} else {
					subscription = purchases.subscriptions().get(GOOGLE_PACKAGE_NAME, sku, pt).execute();
				}
				
				updateSubscriptionDb(userid, pt, sku, startTime, expireTime, tm, subscription);
			} catch (IOException e) {
				
				boolean gone = false;
				if(e instanceof GoogleJsonResponseException) {
					gone = ((GoogleJsonResponseException) e).getStatusCode() == 410;
				}
					
				String reason = null;
				String kind = "";
				if (!pt.contains(".AO")) {
					reason = "invalid purchase token " + e.getMessage();
					kind = "invalid";
				} else if(gone) {
					kind = "gone";
					if(expireTime == null) {
						reason = "subscription expired.";
					} else if(expireTime != null && ((tm - expireTime.getTime()) > 15 * DAY) ) {
						reason = String.format("subscription expired more than %.1f days ago", (tm - expireTime.getTime())
								/ (DAY * 1.0d));
					}
						
					
				}
				if (reason != null) {
					delStat.setString(1, kind);
					delStat.setTimestamp(2, new Timestamp(tm));
					delStat.setString(3, userid);
					delStat.setString(4, pt);
					delStat.setString(5, sku);
					delStat.addBatch();
					deletions++;
					System.out.println(String.format(
							"!! Clearing invalid subscription: userid=%s, sku=%s. Reason: %s ", userid, sku, reason));
					if (deletions > BATCH_SIZE) {
						delStat.executeUpdate();
						deletions = 0;
					}
				} else {
					System.err.println(String.format("!! Error updating userid %s and sku %s: %s", userid, sku, e.getMessage())) ;
				}
				continue;
			}
			
		}
		if (deletions > 0) {
			delStat.executeBatch();
		}
		if (changes > 0) {
			updSubscrStat.executeBatch();
		}
		if (!conn.getAutoCommit()) {
			conn.commit();
		}
	}

	private void updateSubscriptionDb(String userid, String pt, String sku, Timestamp startTime, Timestamp expireTime,
			long tm, SubscriptionPurchase subscription) throws SQLException {
		boolean updated = false;
		updSubscrStat.setTimestamp(1, new Timestamp(tm));
		if(subscription.getStartTimeMillis() != null) {
			if(startTime != null && Math.abs(startTime.getTime() - subscription.getStartTimeMillis().longValue()) > 10*1000 && 
					startTime.getTime() > 100000*1000l) {
				throw new IllegalArgumentException(String.format("Start timestamp changed %s != %s %s %s", 
						startTime == null ? "" :new Date(startTime.getTime()),
								new Date(subscription.getStartTimeMillis().longValue()), userid, sku));
			}
			updSubscrStat.setTimestamp(2, new Timestamp(subscription.getStartTimeMillis()));
			updated = true;
		} else {
			updSubscrStat.setTimestamp(2, startTime);
		}
		if(subscription.getExpiryTimeMillis() != null) {
			if(expireTime == null || Math.abs(expireTime.getTime() - subscription.getExpiryTimeMillis().longValue()) > 10 * 1000) {
				System.out.println(String.format("Expire timestamp changed %s != %s for %s %s", 
						expireTime == null ? "" :new Date(expireTime.getTime()), 
								new Date(subscription.getExpiryTimeMillis().longValue()), userid, sku));
			}
			updSubscrStat.setTimestamp(3, new Timestamp(subscription.getExpiryTimeMillis()));
			updated = true;
		} else {
			updSubscrStat.setTimestamp(3, expireTime);
		}
		if(subscription.getAutoRenewing() == null) {
			updSubscrStat.setNull(4, Types.BOOLEAN);
		} else {
			updSubscrStat.setBoolean(4, subscription.getAutoRenewing());	
		}
		updSubscrStat.setString(5, subscription.getKind());
		updSubscrStat.setBoolean(6, true);
		updSubscrStat.setString(7, userid);
		updSubscrStat.setString(8, pt);
		updSubscrStat.setString(9, sku);
		System.out.println(String.format("%s for %s %s start %s expire %s",
				updated ? "Updates " :  "No changes ",
				userid, sku,
				startTime == null ? "" : new Date(startTime.getTime()),
				expireTime == null ? "" : new Date(expireTime.getTime())
		));
		if(updated) {
			updSubscrStat.addBatch();
			changes++;
			if (changes > BATCH_SIZE) {
				updSubscrStat.executeBatch();
				changes = 0;
			}
		}
	}


	private static AndroidPublisher getPublisherApi(String file) throws JSONException, IOException {
		Properties properties = new Properties();
		properties.load(new FileInputStream(file));
		GOOGLE_CLIENT_CODE = properties.getProperty("GOOGLE_CLIENT_CODE");
		GOOGLE_CLIENT_ID = properties.getProperty("GOOGLE_CLIENT_ID");
		GOOGLE_CLIENT_SECRET = properties.getProperty("GOOGLE_CLIENT_SECRET");
		GOOGLE_REDIRECT_URI = properties.getProperty("GOOGLE_REDIRECT_URI");
		TOKEN = properties.getProperty("TOKEN");
		
		String token = TOKEN;//getRefreshToken();
		String accessToken = getAccessToken(token);
		TokenResponse tokenResponse = new TokenResponse();
		
//		System.out.println("refresh token=" + token);
//		System.out.println("access token=" + accessToken);
		
		tokenResponse.setAccessToken(accessToken);
		tokenResponse.setRefreshToken(token);
		tokenResponse.setExpiresInSeconds(3600L);
		tokenResponse.setScope("https://www.googleapis.com/auth/androidpublisher");
		tokenResponse.setTokenType("Bearer");

		HttpRequestInitializer credential = new GoogleCredential.Builder().setTransport(HTTP_TRANSPORT)
				.setJsonFactory(JSON_FACTORY).setClientSecrets(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET).build()
				.setFromTokenResponse(tokenResponse);

		AndroidPublisher publisher = new AndroidPublisher.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
				.setApplicationName(GOOGLE_PRODUCT_NAME).build();

		return publisher;
	}
	

	private static void test(AndroidPublisher publisher, String subscriptionId,String purchaseToken) {
		try {

			com.google.api.services.androidpublisher.AndroidPublisher.Inappproducts.List lst = publisher.inappproducts().list(GOOGLE_PACKAGE_NAME_FREE);
			InappproductsListResponse response = lst.execute();
			for(InAppProduct p : response.getInappproduct()) {
				System.out.println("SKU="+p.getSku() +
						" type=" + p.getPurchaseType() +
						" LNG=" + p.getDefaultLanguage() +
						//" P="+p.getPrices()+
						" Period=" +p.getSubscriptionPeriod() + " Status="+p.getStatus());
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
		HttpPost post = new HttpPost("https://www.googleapis.com/oauth2/v4/token");
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
	
	public static String getRefreshToken() {

		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost("https://accounts.google.com/o/oauth2/token");
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

			JSONObject json = new JSONObject(buffer.toString());
			String refreshToken = json.getString("refresh_token");
			return refreshToken;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

}
