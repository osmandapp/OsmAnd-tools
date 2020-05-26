package net.osmand.live.subscriptions;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import net.osmand.osm.io.NetworkUtils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class ReceiptValidationHelper {

	private final static String PRODUCTION_URL = "https://buy.itunes.apple.com/verifyReceipt";
	private final static String BUNDLE_ID = "net.osmand.maps";

	public final static int NO_RESPONSE_ERROR_CODE = 1100;
	// https://developer.apple.com/documentation/appstorereceipts/status
	// The user account cannot be found or has been deleted.
	public static final int USER_GONE = 21010;
	
	// This receipt is from the test environment, but it was sent to the production environment for verification.
	public static final int SANDBOX_ERROR_CODE_TEST = 21007;
	
	
	
	
	
	public static class ReceiptResult {
		public boolean result;
		public int error;
		public JsonObject response;
	}

	public ReceiptResult loadReceipt(String receipt) {
		ReceiptResult result = new ReceiptResult();

		JsonObject receiptObj = new JsonObject();
		receiptObj.addProperty("receipt-data", receipt);
		receiptObj.addProperty("password", System.getenv().get("IOS_SUBSCRIPTION_SECRET"));

		String jsonAnswer = postReceiptJson(receiptObj);
		if (jsonAnswer != null) {
			JsonObject responseObj = new JsonParser().parse(jsonAnswer).getAsJsonObject();
			JsonElement statusElement = responseObj.get("status");
			int status = statusElement != null ? statusElement.getAsInt() : 0;
			if (status > 0) {
				result.error = status;
			} else {
				result.result = true;
				result.response = responseObj;
			}
		} else {
			result.error = NO_RESPONSE_ERROR_CODE;
		}
		return result;
	}

	public List<InAppReceipt> parseInAppReceipts(JsonObject receiptObj) {
		List<InAppReceipt> result = new ArrayList<ReceiptValidationHelper.InAppReceipt>();
		JsonElement pendingInfo = receiptObj.get("pending_renewal_info");
		Map<String, Boolean> autoRenewStatus = new TreeMap<String, Boolean>();
		if (pendingInfo != null) {
			JsonArray ar = pendingInfo.getAsJsonArray();
			for (int i = 0; i < ar.size(); i++) {
				JsonObject o = ar.get(i).getAsJsonObject();
				// "auto_renew_product_id", "original_transaction_id", "product_id", "auto_renew_status"
				JsonElement renewStatus = o.get("auto_renew_status");
				JsonElement txId = o.get("original_transaction_id");
				if (renewStatus != null && txId != null) {
					autoRenewStatus.put(txId.getAsString(), renewStatus.getAsString().equals("1"));
				}
			}
		}
		String bundleId = receiptObj.get("receipt").getAsJsonObject().get("bundle_id").getAsString();
		if (bundleId.equals(BUNDLE_ID)) {
			JsonElement receiptInfo = receiptObj.get("latest_receipt_info");
			if (receiptInfo != null) {
				JsonArray receiptArray = receiptInfo.getAsJsonArray();
				for (JsonElement elem : receiptArray) {
					JsonObject recObj = elem.getAsJsonObject();
					String transactionId = recObj.get("original_transaction_id").getAsString();
					InAppReceipt receipt = new InAppReceipt();
					if (autoRenewStatus.containsKey(transactionId)) {
						receipt.autoRenew = autoRenewStatus.get(transactionId);
					}
					for (Map.Entry<String, JsonElement> entry : recObj.entrySet()) {
						receipt.fields.put(entry.getKey(), entry.getValue().getAsString());
					}
					result.add(receipt);
				}
			}
		}
		return result;
	}

	private static String postReceiptJson(JsonObject json) {
		HttpURLConnection connection = null;
		try {
			connection = NetworkUtils.getHttpURLConnection(ReceiptValidationHelper.PRODUCTION_URL);
			connection.setRequestProperty("Accept-Charset", "UTF-8");
//			connection.setRequestProperty("User-Agent", "OsmAnd Server 1.0");
			connection.setConnectTimeout(30000);
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

			String jsonString = json.toString();
			byte[] out = jsonString.getBytes(StandardCharsets.UTF_8);
			int length = out.length;
			connection.setRequestProperty("Content-Length", String.valueOf(length));
			connection.setFixedLengthStreamingMode(length);
			OutputStream output = new BufferedOutputStream(connection.getOutputStream());
			output.write(out);
			output.flush();
			output.close();

			if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
				System.err.println(String.format("Error while posting json: %s", connection.getResponseMessage()));
			} else {
				StringBuilder responseBody = new StringBuilder();
				responseBody.setLength(0);
				InputStream i = connection.getInputStream();
				if (i != null) {
					BufferedReader in = new BufferedReader(new InputStreamReader(i, StandardCharsets.UTF_8), 256);
					String s;
					boolean f = true;
					while ((s = in.readLine()) != null) {
						if (!f) {
							responseBody.append("\n");
						} else {
							f = false;
						}
						responseBody.append(s);
					}
					try {
						in.close();
						i.close();
					} catch (Exception e) {
						// ignore exception
					}
				}
				return responseBody.toString();
			}
		} catch (NullPointerException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
		return null;
	}

	public static class InAppReceipt {
		public Boolean autoRenew;
		public Map<String, String> fields = new HashMap<>();

		public String getProductId() {
			return fields.get("product_id");
		}

		public boolean isSubscription() {
			String productId = getProductId();
			return productId != null && productId.contains("subscription");
		}
	}
}
