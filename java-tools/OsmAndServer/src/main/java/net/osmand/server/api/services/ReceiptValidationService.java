package net.osmand.server.api.services;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ReceiptValidationService {

	private static final Log LOGGER = LogFactory.getLog(ReceiptValidationService.class);

	private final static String SANDBOX_URL = "https://sandbox.itunes.apple.com/verifyReceipt";
	private final static String PRODUCTION_URL = "https://buy.itunes.apple.com/verifyReceipt";
	private final static String BUNDLE_ID = "net.osmand.maps";

	public final static int CANNOT_LOAD_RECEIPT_STATUS = 50000;
	public final static int ALL_SUBSCRIPTIONS_EXPIRED_STATUS = 100;
	public final static int NO_SUBSCRIPTIONS_FOUND_STATUS = 110;
	public final static int INCONSISTENT_RECEIPT_STATUS = 200;
	public final static int USER_NOT_FOUND_STATUS = 300;

	public static class InAppReceipt {
		public Map<String, String> fields = new HashMap<>();

		public String getProductId() {
			return fields.get("product_id");
		}

		public boolean isSubscription() {
			String productId = getProductId();
			return productId != null && productId.contains("subscription");
		}
	}

	@NonNull
	public Map<String, Object> validateReceipt(@NonNull JsonObject receiptObj, @NonNull List<Map<String, String>> activeSubscriptions) {
		try {
			int status = receiptObj.get("status").getAsInt();
			if (status != 0) {
				return mapStatus(status);
			}
			return checkValidation(receiptObj, activeSubscriptions);
		} catch (Exception e) {
			LOGGER.error(e);
			return mapStatus(CANNOT_LOAD_RECEIPT_STATUS);
		}
	}

	@Nullable
	public Map<String, InAppReceipt> loadInAppReceipts(@NonNull JsonObject receiptObj) {
		Map<String, InAppReceipt> result = null;
		int status = receiptObj.get("status").getAsInt();
		if (status == 0) {
			String bundleId = receiptObj.get("receipt").getAsJsonObject().get("bundle_id").getAsString();
			if (bundleId.equals(BUNDLE_ID)) {
				result = new HashMap<>();
				JsonElement receiptInfo = receiptObj.get("latest_receipt_info");
				if (receiptInfo != null) {
					JsonArray receiptArray = receiptInfo.getAsJsonArray();
					for (JsonElement elem : receiptArray) {
						JsonObject recObj = elem.getAsJsonObject();
						String transactionId = recObj.get("original_transaction_id").getAsString();
						InAppReceipt receipt = new InAppReceipt();
						for (Map.Entry<String, JsonElement> entry : recObj.entrySet()) {
							receipt.fields.put(entry.getKey(), entry.getValue().getAsString());
						}
						result.put(transactionId, receipt);
					}
				}
			}
		}
		return result;
	}

	@Nullable
	public JsonObject loadReceiptJsonObject(String receipt, boolean sandbox) {
		JsonObject receiptObj = new JsonObject();
		receiptObj.addProperty("receipt-data", receipt);
		receiptObj.addProperty("password", System.getenv().get("IOS_SUBSCRIPTION_SECRET"));
		String receiptWithSecret = receiptObj.toString();

		RestTemplate restTemplate = new RestTemplate();
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);

		HttpEntity<String> entity = new HttpEntity<>(receiptWithSecret, headers);
		String jsonAnswer = restTemplate
				.postForObject(sandbox ? SANDBOX_URL : PRODUCTION_URL, entity, String.class);

		if (jsonAnswer != null) {
			JsonObject responseObj = new JsonParser().parse(jsonAnswer).getAsJsonObject();
			JsonElement statusElement = responseObj.get("status");
			int status = statusElement != null ? statusElement.getAsInt() : 0;
			if (status == 21007 && !sandbox) {
				return loadReceiptJsonObject(receipt, true);
			}
			return responseObj;

		}
		return null;
	}

	@NonNull
	private HashMap<String, Object> checkValidation(@NonNull JsonObject receiptObj, @NonNull List<Map<String, String>> activeSubscriptions) {
		HashMap<String, Object> result = new HashMap<>();
		//To be determined with which field to compare
		String bundleId = receiptObj.get("receipt").getAsJsonObject().get("bundle_id").getAsString();
		JsonArray latestReceiptInfoArray = receiptObj.get("latest_receipt_info").getAsJsonArray();
		if (bundleId.equals(BUNDLE_ID)) {
			if (latestReceiptInfoArray.size() > 0) {
				result.put("result", true);
				List<String> inAppArray = new ArrayList<>();
				for (JsonElement jsonElement : latestReceiptInfoArray) {
					Map<String, String> subscriptionObj = new HashMap<>();
					JsonObject receipt = jsonElement.getAsJsonObject();
					String productId = receipt.get("product_id").getAsString();
					subscriptionObj.put("product_id", productId);
					JsonElement expiresDateElement = receipt.get("expires_date_ms");
					if (expiresDateElement != null) {
						long expiresDateMs = expiresDateElement.getAsLong();
						if (expiresDateMs > System.currentTimeMillis()) {
							//Subscription is valid (active)
							subscriptionObj.put("expiration_date", Long.toString(expiresDateMs));
							activeSubscriptions.add(subscriptionObj);
						}
					} else {
						inAppArray.add(productId);
					}
				}
				result.put("in_apps", inAppArray);
				result.put("subscriptions", activeSubscriptions);
				result.put("status", 0);
				return result;
			} else {
				//No items in latest_receipt, considered valid
				result.put("result", false);
				result.put("message", "All expired");
				result.put("status", ALL_SUBSCRIPTIONS_EXPIRED_STATUS);
				return result;
			}
		} else {
			result.put("result", false);
			result.put("message", "Inconsistency in receipt information");
			result.put("status", INCONSISTENT_RECEIPT_STATUS);
			return result;
		}
	}

	private HashMap<String, Object> mapStatus(int status) {
		HashMap<String, Object> result = new HashMap<>();
		switch (status) {
			case 0:
				result.put("result", true);
				result.put("message", "Success");
				return result;
			case 21000:
				result.put("message", "App store could not read");
				break;
			case 21002:
				result.put("message", "Data was malformed");
				break;
			case 21003:
				result.put("message", "Receipt not authenticated");
				break;
			case 21004:
				result.put("message", "Shared secret does not match");
				break;
			case 21005:
				result.put("message", "Receipt server unavailable");
				break;
			case 21006:
				result.put("message", "Receipt valid but sub expired");
				break;
			case 21007:
				result.put("message", "Sandbox receipt sent to Production environment");
				break;
			case 21008:
				result.put("message", "Production receipt sent to Sandbox environment");
				break;

			case CANNOT_LOAD_RECEIPT_STATUS:
				result.put("message", "Cannot load receipt json");
				break;
			default:
				result.put("message", "Unknown error: status code");
		}
		result.put("result", false);
		result.put("status", status);
		return result;
	}
}