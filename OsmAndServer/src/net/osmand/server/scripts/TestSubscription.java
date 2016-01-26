package net.osmand.server.scripts;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.model.InAppProduct;
import com.google.api.services.androidpublisher.model.InappproductsListResponse;
import com.google.api.services.androidpublisher.model.SubscriptionPurchase;


public class TestSubscription {
	
	
	// init one time
	private static String GOOGLE_CLIENT_CODE="";
	private static String GOOGLE_CLIENT_ID = "";
	private static String GOOGLE_CLIENT_SECRET = "";
	private static String GOOGLE_REDIRECT_URI = "";
	private static String TOKEN = "";
	
	
	private static final String GOOGLE_PRODUCT_NAME = "OsmAnd+";
	private static final String GOOGLE_PACKAGE_NAME = "net.osmand.plus";
	private static HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static JsonFactory JSON_FACTORY = new com.google.api.client.json.jackson2.JacksonFactory();

	public static void main(String[] args) throws JSONException, IOException {
		AndroidPublisher publisher = getPublisherApi(args[0]);
		String subscriptionId = "osm_live_subscription_1";
		String purchaseToken = "bbihnopbfegmppomnnkkfnoa.AO-J1Ox1O0DnhpYFM-zhqREI2QQxoFBAQIRlOPBvXu6ni2pkUzme-X6GT-bLcyfmSwEDCPj_WSn1g8752zjHx6zJVVoiGdzN3zVg8hBOwcZRu9ufq8Qd2eK0DL5qPLD5FoC7lLxjgyEf";
		test(publisher, subscriptionId, purchaseToken);
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
		System.out.println("refresh token=" + token);
		System.out.println("access token=" + accessToken);
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

			com.google.api.services.androidpublisher.AndroidPublisher.Inappproducts.List lst = publisher.inappproducts().list(GOOGLE_PACKAGE_NAME);
			InappproductsListResponse response = lst.execute();
			for(InAppProduct p : response.getInappproduct()) {
				System.out.println("SKU="+p.getSku() +
						" type=" + p.getPurchaseType() +
						" LNG=" + p.getDefaultLanguage() +
						//" P="+p.getPrices()+
						" Period=" +p.getSubscriptionPeriod() + " Status="+p.getStatus());
			}
			AndroidPublisher.Purchases purchases = publisher.purchases();
			SubscriptionPurchase subscription = purchases.subscriptions().get(GOOGLE_PACKAGE_NAME, subscriptionId, purchaseToken).execute();
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
		HttpPost post = new HttpPost("https://accounts.google.com/o/oauth2/token");
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
