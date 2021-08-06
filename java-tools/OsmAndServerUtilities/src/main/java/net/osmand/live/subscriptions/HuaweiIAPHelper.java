package net.osmand.live.subscriptions;

import net.osmand.util.Algorithms;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class HuaweiIAPHelper {

	private static final String CLIENT_ID = "101486545";
	private static final String TOKEN_URL = "https://oauth-login.cloud.huawei.com/oauth2/v3/token";

	// Unknown
	public static final int RESPONSE_CODE_UNKNOWN = -1;

	// Susccess
	public static final int RESPONSE_CODE_SUCCESS = 0;

	// The parameters passed to the API are invalid. This error may also indicate that an agreement is not signed or
	// parameters are not set correctly for the in-app purchase billing in HUAWEI IAP, or the required permission is not in the list.
	// Check whether the request parameters are correctly set.
	// Check whether other relevant settings are correctly defined.
	public static final int RESPONSE_CODE_PARAMETERS_ERROR = 5;

	// A critical error occurs during API operations.
	// Fix the error based on the error information in the response.
	public static final int RESPONSE_CODE_CRITICAL_ERROR = 6;

	// A user failed to consume or confirm a product because the user does not own the product.
	// First, make sure that consuming or confirming a product is performed for a user only
	// when the user has successfully purchased it, that is, already owned the product.
	// Then, you also need to ensure that the input parameters in the API call request are correct.
	public static final int RESPONSE_CODE_USER_CONSUME_ERROR = 8;

	// The product cannot be consumed or confirmed because it has been consumed or confirmed.
	// Check why repeated consumption or confirmation occurs and further optimize the project logic.
	// If process confirmation and suggestions are required, contact Huawei technical support.
	public static final int RESPONSE_CODE_ALREADY_CONSUMED_ERROR = 9;

	// The user account is abnormal. For example, the user has been deregistered.
	// Use another account or register a new account if no other account is available.
	public static final int RESPONSE_CODE_USER_ACCOUNT_ERROR = 11;

	// The order does not exist. The order in this query may be a historical order.
	// Token verification is unnecessary for a historical order under normal circumstances,
	// and HUAWEI IAP only allows for token verification for the latest order of a product.
	// Check your integration process to make sure it complies with that described in the development guide.
	public static final int RESPONSE_CODE_ORDER_NOT_EXIST_ERROR = 12;

	/**
	 * The accessToken.
	 */
	private String accessToken = null;
	private long accessTokenExpireTime = 0;

	
	public HuaweiIAPHelper() {
	}
	
	public String getAccessToken() throws IOException {
		if (accessTokenExpireTime < System.currentTimeMillis()) {
			// fetch accessToken
			String grantType = "client_credentials";
			String clientSecret = System.getenv("HUAWEI_CLIENT_SECRET");
			String clientID = System.getenv("HUAWEI_CLIENT_ID");
			if (Algorithms.isEmpty(clientID)) {
				clientID = CLIENT_ID;
			}
			String msgBody = MessageFormat.format("grant_type={0}&client_secret={1}&client_id={2}", grantType,
					URLEncoder.encode(clientSecret, "UTF-8"), clientID);
			String response = httpPost(TOKEN_URL, "application/x-www-form-urlencoded; charset=UTF-8", msgBody, 10000, 10000, null);
			JSONObject obj = new JSONObject(response);
			accessToken = obj.getString("access_token");
			accessTokenExpireTime = System.currentTimeMillis() + obj.getLong("expires_in") * 1000 - 10000;
		}
		return accessToken;
	}

	private Map<String, String> buildAuthorization(String appAccessToken) {
		String oriString = MessageFormat.format("APPAT:{0}", appAccessToken);
		String authorization =
				MessageFormat.format("Basic {0}", Base64.getEncoder().encodeToString(oriString.getBytes(StandardCharsets.UTF_8)));
		Map<String, String> headers = new HashMap<>();
		headers.put("Authorization", authorization);
		headers.put("Content-Type", "application/json; charset=UTF-8");
		return headers;
	}

	public static class HuaweiJsonResponseException extends IOException {
		private static final long serialVersionUID = 5398592754615340627L;

		public final int responseCode;
		public final String responseMessage;

		public HuaweiJsonResponseException(int responseCode, String responseMessage) {
			super(responseMessage);
			this.responseCode = responseCode;
			this.responseMessage = responseMessage;
		}
	}

	public static class HuaweiSubscription {

		public final JSONObject dataJson;

		public final Boolean autoRenewing;
		public final Boolean subIsvalid;
		public final String orderId;
		public final String lastOrderId;
		public final String packageName;
		public final String applicationId;
		public final String productId;
		public final Integer kind;
		public final String productName;
		public final String productGroup;
		public final Long purchaseTime;
		public final Long oriPurchaseTime;
		public final Integer purchaseState;
		public final String developerPayload;
		public final String purchaseToken;
		public final Integer purchaseType;
		public final String currency;
		public final Long price;
		public final String country;
		public final String subscriptionId;
		public final Integer quantity;
		public final Long daysLasted;
		public final Long numOfPeriods;
		public final Long numOfDiscount;
		public final Long expirationDate;
		public final Integer retryFlag;
		public final Integer introductoryFlag;
		public final Integer trialFlag;
		public final Integer renewStatus;
		public final Long renewPrice;
		public final Integer cancelledSubKeepDays;
		public final String payOrderId;
		public final String payType;
		public final Integer confirmed;

		public HuaweiSubscription(String inappPurchaseData) {
			this.dataJson = new JSONObject(inappPurchaseData);
			autoRenewing = dataJson.has("autoRenewing") ? dataJson.optBoolean("autoRenewing") : null;
			subIsvalid = dataJson.has("subIsvalid") ? dataJson.optBoolean("subIsvalid") : null;
			orderId = dataJson.optString("orderId", null);
			lastOrderId = dataJson.optString("lastOrderId", null);
			packageName = dataJson.optString("packageName", null);
			applicationId = dataJson.optString("applicationId", null);
			productId = dataJson.optString("productId", null);
			kind = dataJson.has("kind") ? dataJson.optInt("kind") : null;
			productName = dataJson.optString("productName", null);
			productGroup = dataJson.optString("productGroup", null);
			purchaseTime = dataJson.has("purchaseTime") ? dataJson.optLong("purchaseTime") : null;
			oriPurchaseTime = dataJson.has("oriPurchaseTime") ? dataJson.optLong("oriPurchaseTime") : null;
			purchaseState = dataJson.has("purchaseState") ? dataJson.optInt("purchaseState") : null;
			developerPayload = dataJson.optString("developerPayload", null);
			purchaseToken = dataJson.optString("purchaseToken", null);
			purchaseType = dataJson.has("purchaseType") ? dataJson.optInt("purchaseType") : null;
			currency = dataJson.optString("currency", null);
			price = dataJson.has("price") ? dataJson.optLong("price") : null;
			country = dataJson.optString("country", null);
			subscriptionId = dataJson.optString("subscriptionId", null);
			quantity = dataJson.has("quantity") ? dataJson.optInt("quantity") : null;
			daysLasted = dataJson.has("daysLasted") ? dataJson.optLong("daysLasted") : null;
			numOfPeriods = dataJson.has("numOfPeriods") ? dataJson.optLong("numOfPeriods") : null;
			numOfDiscount = dataJson.has("numOfDiscount") ? dataJson.optLong("numOfDiscount") : null;
			expirationDate = dataJson.has("expirationDate") ? dataJson.optLong("expirationDate") : null;
			retryFlag = dataJson.has("retryFlag") ? dataJson.optInt("retryFlag") : null;
			introductoryFlag = dataJson.has("introductoryFlag") ? dataJson.optInt("introductoryFlag") : null;
			trialFlag = dataJson.has("trialFlag") ? dataJson.optInt("trialFlag") : null;
			renewStatus = dataJson.has("renewStatus") ? dataJson.optInt("renewStatus") : null;
			renewPrice = dataJson.has("renewPrice") ? dataJson.optLong("renewPrice") : null;
			cancelledSubKeepDays = dataJson.has("cancelledSubKeepDays") ? dataJson.optInt("cancelledSubKeepDays") : null;
			payOrderId = dataJson.optString("payOrderId", null);
			payType = dataJson.optString("payType", null);
			confirmed = dataJson.has("confirmed") ? dataJson.optInt("confirmed") : null;


			System.out.println("---------- BEGIN ----------");
			System.out.println(toString());
			System.out.println("----------- END -----------");
		}

		@Override
		public String toString() {
			return dataJson.toString();
		}
	}

	public HuaweiSubscription getHuaweiSubscription(String subscriptionId, String purchaseToken) throws IOException {
		// fetch the App Level AccessToken
		String accessToken = getAccessToken();
		// construct the Authorization in Header
		Map<String, String> headers = buildAuthorization(accessToken);

		// pack the request body
		Map<String, String> bodyMap = new HashMap<>();
		if (subscriptionId != null) {
			bodyMap.put("subscriptionId", subscriptionId);
		}
		bodyMap.put("purchaseToken", purchaseToken);

		String msgBody = new JSONObject(bodyMap).toString();

		String response = httpPost("https://subscr-dre.iap.hicloud.com/sub/applications/v2/purchases/get",
				"application/json; charset=UTF-8", msgBody, 10000, 10000, headers);

		JSONObject responseJson = new JSONObject(response);
		int responseCode;
		try {
			responseCode = Integer.parseInt(responseJson.optString("responseCode", "" + RESPONSE_CODE_UNKNOWN));
		} catch (NumberFormatException e) {
			responseCode = RESPONSE_CODE_UNKNOWN;
		}
		String responseMessage = responseJson.optString("responseMessage", null);
		if (responseCode != RESPONSE_CODE_SUCCESS) {
			throw new HuaweiJsonResponseException(responseCode, responseMessage);
		}
		String inappPurchaseData = responseJson.optString("inappPurchaseData", null);
		if (Algorithms.isEmpty(inappPurchaseData)) {
			throw new IOException(String.format("No purchase data returned for subscriptionId: '%s': %s", subscriptionId, purchaseToken));
		}
		return new HuaweiSubscription(inappPurchaseData);
	}
	
	private static String httpPost(String httpUrl, String contentType, String data, int connectTimeout, int readTimeout,
								   Map<String, String> headers) throws IOException {
		OutputStream output = null;
		InputStream in = null;
		HttpURLConnection urlConnection = null;
		BufferedReader bufferedReader = null;
		InputStreamReader inputStreamReader = null;
		try {
			URL url = new URL(httpUrl);
			urlConnection = (HttpURLConnection) url.openConnection();
			urlConnection.setRequestMethod("POST");
			urlConnection.setDoOutput(true);
			urlConnection.setDoInput(true);
			urlConnection.setRequestProperty("Content-Type", contentType);
			if (headers != null) {
				for (String key : headers.keySet()) {
					urlConnection.setRequestProperty(key, headers.get(key));
				}
			}
			urlConnection.setConnectTimeout(connectTimeout);
			urlConnection.setReadTimeout(readTimeout);
			urlConnection.connect();

			// POST data
			output = urlConnection.getOutputStream();
			output.write(data.getBytes(StandardCharsets.UTF_8));
			output.flush();

			// read response
			if (urlConnection.getResponseCode() < 400) {
				in = urlConnection.getInputStream();
			} else {
				in = urlConnection.getErrorStream();
			}

			inputStreamReader = new InputStreamReader(in, StandardCharsets.UTF_8);
			bufferedReader = new BufferedReader(inputStreamReader);
			StringBuilder strBuf = new StringBuilder();
			String str;
			while ((str = bufferedReader.readLine()) != null) {
				strBuf.append(str);
			}
			return strBuf.toString();
		} finally {
			if (bufferedReader != null) {
				bufferedReader.close();
			}
			if (inputStreamReader != null) {
				inputStreamReader.close();
			}
			if (in != null) {
				in.close();
			}
			if (urlConnection != null) {
				urlConnection.disconnect();
			}
		}
	}
}
