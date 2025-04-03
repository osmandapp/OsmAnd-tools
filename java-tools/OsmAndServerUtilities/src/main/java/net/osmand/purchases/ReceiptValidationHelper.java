package net.osmand.purchases;

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

	public static final String FIELD_BUNDLE_ID = "bundle_id";
	public static final String FIELD_PRODUCT_ID = "product_id";
	public static final String FIELD_ORIGINAL_TRANSACTION_ID = "original_transaction_id";
	public static final String FIELD_TRANSACTION_ID = "transaction_id";
	public static final String FIELD_STATUS = "status";
	public static final String FIELD_RECEIPT = "receipt";
	public static final String FIELD_LATEST_RECEIPT_INFO = "latest_receipt_info";

	private final static String PRODUCTION_URL = "https://buy.itunes.apple.com/verifyReceipt";
	private final static String SANDBOX_URL = "https://sandbox.itunes.apple.com/verifyReceipt";

	public final static String IOS_MAPS_BUNDLE_ID = "net.osmand.maps";

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


	public static void main(String[] args) {
		// load ios subscription
		String prevReceipt = "";
//		String currReceipt = "";

		ReceiptValidationHelper helper = new ReceiptValidationHelper();
		ReceiptResult loadReceipt = helper.loadReceipt(prevReceipt, false);
		System.out.println(loadReceipt.error + " " + loadReceipt.result + " " + loadReceipt.response);

//		loadReceipt = helper.loadReceipt(currReceipt, false);
//		System.out.println(loadReceipt.error + " " + loadReceipt.result + " " + loadReceipt.response);
	}

	public ReceiptResult loadReceipt(String receipt, boolean sandbox) {
		ReceiptResult result = new ReceiptResult();

		JsonObject receiptObj = new JsonObject();
		receiptObj.addProperty("receipt-data", receipt);
		receiptObj.addProperty("password", System.getenv().get("IOS_SUBSCRIPTION_SECRET"));

		String jsonAnswer = postReceiptJson(receiptObj, sandbox);
		if (jsonAnswer != null) {
			JsonObject responseObj = new JsonParser().parse(jsonAnswer).getAsJsonObject();
			JsonElement statusElement = responseObj.get(FIELD_STATUS);
			int status = statusElement != null ? statusElement.getAsInt() : 0;
			if (status > 0) {
				result.error = status;
			} else {
				result.result = true;
			}
			result.response = responseObj;
		} else {
			result.error = NO_RESPONSE_ERROR_CODE;
		}
		return result;
	}

	public static List<InAppReceipt> parseInAppReceipts(JsonObject receiptObj) {
		List<InAppReceipt> result = new ArrayList<ReceiptValidationHelper.InAppReceipt>();
		JsonElement pendingInfo = receiptObj.get("pending_renewal_info");
		Map<String, Boolean> autoRenewStatus = new TreeMap<String, Boolean>();
		if (pendingInfo != null) {
			JsonArray ar = pendingInfo.getAsJsonArray();
			for (int i = 0; i < ar.size(); i++) {
				JsonObject o = ar.get(i).getAsJsonObject();
				// "auto_renew_product_id", "original_transaction_id", "product_id", "auto_renew_status"
				JsonElement renewStatus = o.get("auto_renew_status");
				JsonElement txId = o.get(FIELD_ORIGINAL_TRANSACTION_ID);
				if (renewStatus != null && txId != null) {
					autoRenewStatus.put(txId.getAsString(), renewStatus.getAsString().equals("1"));
				}
			}
		}
		String bundleId = receiptObj.get(FIELD_RECEIPT).getAsJsonObject().get(FIELD_BUNDLE_ID).getAsString();
		if (bundleId.equals(IOS_MAPS_BUNDLE_ID)) {
			JsonElement receiptInfo = receiptObj.get(FIELD_LATEST_RECEIPT_INFO);
			if (receiptInfo != null) {
				JsonArray receiptArray = receiptInfo.getAsJsonArray();
				for (JsonElement elem : receiptArray) {
					JsonObject recObj = elem.getAsJsonObject();
					String transactionId = recObj.get(FIELD_ORIGINAL_TRANSACTION_ID).getAsString();
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

	private static String postReceiptJson(JsonObject json, boolean sandbox) {
		HttpURLConnection connection = null;
		try {
			connection = NetworkUtils.getHttpURLConnection(sandbox ? ReceiptValidationHelper.SANDBOX_URL : ReceiptValidationHelper.PRODUCTION_URL);
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
		public Map<String, String> fields = new HashMap<>();
		public Boolean autoRenew;

		public String getProductId() {
			return fields.get(FIELD_PRODUCT_ID);
		}

		public String getOrderId() {
			return fields.get(FIELD_ORIGINAL_TRANSACTION_ID);
		}

		public boolean isSubscription() {
			String productId = getProductId();
			return productId != null && productId.contains("subscription");
		}
	}
}
