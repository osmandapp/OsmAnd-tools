package net.osmand.live.subscriptions;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.*;

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
	
	private static HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static JsonFactory JSON_FACTORY = new com.google.api.client.json.jackson2.JacksonFactory();

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
		// supporters_subscription
		// supporters_device_sub

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT userid, sku, purchaseToken, checktime, starttime, expiretime "
				+ "FROM supporters_device_sub S where (valid is null or valid=true) ");
//		ResultSet rs = conn.createStatement().executeQuery(
//				"SELECT * FROM ( " +
//				"  SELECT DISTINCT userid, sku,  " +
//				"	first_value(purchaseToken) over (partition by userid, sku order by checktime desc) purchaseToken, " +
//				"	first_value(checktime) over (partition by userid, sku order by checktime desc) checktime, " +
//				"	first_value(autorenewing) over (partition by userid, sku order by checktime desc) autorenewing, " +
//				"	first_value(starttime) over (partition by userid, sku order by checktime desc) starttime, " +
//				"	first_value(kind) over (partition by userid, sku order by checktime desc) kind, " +
//				"	first_value(expiretime) over (partition by userid, sku order by checktime desc) expiretime " +
//				"  FROM supporters_subscription ) a " +
//				(verifyAll? ";" : "WHERE kind = '' or kind is null;"));
		
		queryPurchases(publisher, conn, rs, verifyAll);
	}
	
	private static void queryPurchases(AndroidPublisher publisher, Connection conn, ResultSet rs, boolean verifyAll) throws SQLException {
		PreparedStatement upd = conn
				.prepareStatement("UPDATE supporters_device_sub SET checktime = ?, starttime = ?, expiretime = ?, autorenewing = ?, kind = ?, valid = ? " +
							" WHERE userid = ? and purchaseToken = ? and sku = ?");
		
		PreparedStatement delStatement = conn
				.prepareStatement("UPDATE supporters_device_sub SET valid = false, kind = 'invalid'" +
							" WHERE userid = ? and purchaseToken = ? and sku = ?");

		AndroidPublisher.Purchases purchases = publisher.purchases();
		int changes = 0;
		int deletions = 0;
		while (rs.next()) {
			String userid = rs.getString("userid");
			String pt = rs.getString("purchaseToken");
			String sku = rs.getString("sku");
			Timestamp checkTime = rs.getTimestamp("checktime");
			Timestamp startTime = rs.getTimestamp("starttime");
			Timestamp expireTime = rs.getTimestamp("expiretime");
			long tm = System.currentTimeMillis();
			// TODO skip active
			if(checkTime != null && startTime != null && expireTime != null) {
				if(expireTime.getTime() > tm) {
					System.out.println(String.format("Skip userid=%s, sku=%s - subscribtion is active", userid, sku));
					continue;
				}
			}
			
			SubscriptionPurchase subscription;
			try {
				if(sku.startsWith("osm_free")) {
					subscription = purchases.subscriptions().get(GOOGLE_PACKAGE_NAME_FREE, sku, pt).execute();
				} else {
					subscription = purchases.subscriptions().get(GOOGLE_PACKAGE_NAME, sku, pt).execute();
				}
			} catch (Exception e) {
				if (!pt.contains(".AO")) {
					delStatement.setString(1, userid);
					delStatement.setString(2, pt);
					delStatement.setString(3, sku);
					delStatement.addBatch();
					deletions++;
					System.out.println("!! Clearing invalid subscription: userid=" + userid + " sku=" + sku + ": " + e.getMessage());
				} else {
					System.err.println("!! Error updating userid " + userid + " and sku " + sku + ": " + e.getMessage()) ;
				}
				continue;
			}
			boolean updated = false;
			upd.setTimestamp(1, new Timestamp(tm));
			if(subscription.getStartTimeMillis() != null) {
				if(startTime != null && Math.abs(startTime.getTime() - subscription.getStartTimeMillis().longValue()) > 10*1000 && 
						startTime.getTime() > 100000*1000l) {
					throw new IllegalArgumentException(String.format("Start timestamp changed %s != %s %s %s", 
							startTime == null ? "" :new Date(startTime.getTime()),
									new Date(subscription.getStartTimeMillis().longValue()), userid, sku));
				}
 				upd.setTimestamp(2, new Timestamp(subscription.getStartTimeMillis()));
 				updated = true;
			} else {
				upd.setTimestamp(2, startTime);
			}
			if(subscription.getExpiryTimeMillis() != null) {
				if(expireTime == null || Math.abs(expireTime.getTime() - subscription.getExpiryTimeMillis().longValue()) > 10 * 1000) {
					System.out.println(String.format("Expire timestamp changed %s != %s for %s %s", 
							expireTime == null ? "" :new Date(expireTime.getTime()), 
									new Date(subscription.getExpiryTimeMillis().longValue()), userid, sku));
				}
 				upd.setTimestamp(3, new Timestamp(subscription.getExpiryTimeMillis()));
 				updated = true;
			} else {
				upd.setTimestamp(3, expireTime);
			}
			if(subscription.getAutoRenewing() == null) {
				upd.setNull(4, Types.BOOLEAN);
			} else {
				upd.setBoolean(4, subscription.getAutoRenewing());	
			}
			upd.setString(5, subscription.getKind());
			upd.setBoolean(6, true);
			upd.setString(7, userid);
			upd.setString(8, pt);
			upd.setString(9, sku);
			System.out.println(String.format("%s for %s %s start %s expire %s",
					updated ? "Updates " :  "No changes ",
					userid, sku,
					startTime == null ? "" : new Date(startTime.getTime()),
					expireTime == null ? "" : new Date(expireTime.getTime())
			));
			if(updated) {
				upd.addBatch();
				changes++;
			}
		}
		if (changes > 0) {
			upd.executeBatch();
			if (deletions > 0) {
				delStatement.executeBatch();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
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
