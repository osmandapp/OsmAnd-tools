package net.osmand.server.scripts;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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


public class UpdateSubscriptionImpl {


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
		Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/changeset",
				System.getenv("DB_USER"), System.getenv("DB_PWD"));
		boolean verifyAll = false;
		for (int i = 1; i < args.length; i++) {
			if ("-verifyall".equals(args[i])) {
				verifyAll = true;
			}
		}
		

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT * FROM ( " +
				"  SELECT DISTINCT userid, sku,  " +
				"	first_value(purchaseToken) over (partition by userid, sku order by checktime desc) purchaseToken, " +
				"	first_value(checktime) over (partition by userid, sku order by checktime desc) checktime, " +
				"	first_value(time) over (partition by userid, sku order by checktime desc) time_d, " +
				"	first_value(autorenewing) over (partition by userid, sku order by checktime desc) autorenewing, " +
				"	first_value(starttime) over (partition by userid, sku order by checktime desc) starttime, " +
				"	first_value(kind) over (partition by userid, sku order by checktime desc) kind, " +
				"	first_value(expiretime) over (partition by userid, sku order by checktime desc) expiretime " +
				"  FROM supporters_subscription ) a " +
				(verifyAll? ";" : "WHERE kind = '' or kind is null;"));
		
		queryPurchases(publisher, conn, rs);
	}
	
	private static void queryPurchases(AndroidPublisher publisher, Connection conn, ResultSet rs) throws SQLException {
		PreparedStatement ps = conn
				.prepareStatement("INSERT INTO supporters_subscription(userid, sku, purchaseToken, checktime, autorenewing, starttime, expiretime, kind) "
						+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?)");

		AndroidPublisher.Purchases purchases = publisher.purchases();
		int changes = 0;
		while (rs.next()) {
			String userid = rs.getString("userid");
			String pt = rs.getString("purchasetoken");
			String subscriptionId = rs.getString("sku");
			SubscriptionPurchase subscription;
			try {
				if(subscriptionId.startsWith("osm_free")) {
					subscription = purchases.subscriptions().get(GOOGLE_PACKAGE_NAME_FREE, subscriptionId, pt).execute();
				} else {
					subscription = purchases.subscriptions().get(GOOGLE_PACKAGE_NAME, subscriptionId, pt).execute();
				}
			} catch (Exception e) {
				System.err.println("Error updating userid " + userid + " and sku " + subscriptionId);
				e.printStackTrace();
				continue;
			}
			long tm = System.currentTimeMillis();
			ps.setString(1, userid);
			ps.setString(2, subscriptionId);
			ps.setString(3, pt);
			ps.setLong(4, tm);
			ps.setString(5, subscription.getAutoRenewing() + "");
			ps.setLong(6, subscription.getStartTimeMillis());
			ps.setLong(7, subscription.getExpiryTimeMillis());
			ps.setString(8, subscription.getKind());
			System.out.println("Update " + userid + " time " + tm + " expire " + subscription.getExpiryTimeMillis());
			ps.addBatch();
			changes++;
		}
		if (changes > 0) {
			ps.executeBatch();
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
		
//		tokenResponse.setAccessToken(accessToken);
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
