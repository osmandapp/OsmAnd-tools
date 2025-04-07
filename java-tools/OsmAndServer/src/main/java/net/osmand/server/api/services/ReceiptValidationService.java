package net.osmand.server.api.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import net.osmand.purchases.ReceiptValidationHelper;
import net.osmand.purchases.ReceiptValidationHelper.InAppReceipt;

@Service
public class ReceiptValidationService {

	private static final Log LOGGER = LogFactory.getLog(ReceiptValidationService.class);

	private final static String SANDBOX_URL = "https://sandbox.itunes.apple.com/verifyReceipt";
	private final static String PRODUCTION_URL = "https://buy.itunes.apple.com/verifyReceipt";

	public final static int CANNOT_LOAD_RECEIPT_STATUS = 50000;
	public final static int ALL_SUBSCRIPTIONS_EXPIRED_STATUS = 100;
	public final static int NO_SUBSCRIPTIONS_FOUND_STATUS = 110;
	public final static int INCONSISTENT_RECEIPT_STATUS = 200;
	public final static int USER_NOT_FOUND_STATUS = 300;


	@NonNull
	public Map<String, Object> validateReceipt(@NonNull JsonObject receiptObj, @NonNull List<Map<String, String>> activeSubscriptions) {
		try {
			int status = receiptObj.get(ReceiptValidationHelper.FIELD_STATUS).getAsInt();
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
	public List<InAppReceipt> loadInAppReceipts(@NonNull JsonObject receiptObj) {
		List<InAppReceipt> result = null;
		int status = receiptObj.get(ReceiptValidationHelper.FIELD_STATUS).getAsInt();
		if (status == 0) {
			String bundleId = receiptObj.get(ReceiptValidationHelper.FIELD_RECEIPT).getAsJsonObject().get(ReceiptValidationHelper.FIELD_BUNDLE_ID).getAsString();
			if (bundleId.equals(ReceiptValidationHelper.IOS_MAPS_BUNDLE_ID)) {
				return ReceiptValidationHelper.parseInAppReceipts(receiptObj);
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
			JsonElement statusElement = responseObj.get(ReceiptValidationHelper.FIELD_STATUS);
			int status = statusElement != null ? statusElement.getAsInt() : 0;
			if (status == ReceiptValidationHelper.SANDBOX_ERROR_CODE_TEST && !sandbox) {
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
		String bundleId = receiptObj.get(ReceiptValidationHelper.FIELD_RECEIPT).getAsJsonObject().get(ReceiptValidationHelper.FIELD_BUNDLE_ID).getAsString();
		JsonArray latestReceiptInfoArray = receiptObj.get(ReceiptValidationHelper.FIELD_LATEST_RECEIPT_INFO).getAsJsonArray();
		if (bundleId.equals(ReceiptValidationHelper.IOS_MAPS_BUNDLE_ID)) {
			if (latestReceiptInfoArray.size() > 0) {
				result.put("result", true);
				List<String> inAppArray = new ArrayList<>(); // Original array for backward compatibility
				List<Map<String, String>> inAppsDetailedArray = new ArrayList<>(); // New array with details
				for (JsonElement jsonElement : latestReceiptInfoArray) {
					Map<String, String> subscriptionObj = new HashMap<>();
					JsonObject receipt = jsonElement.getAsJsonObject();
					String productId = addStringField(subscriptionObj, receipt, ReceiptValidationHelper.FIELD_PRODUCT_ID);
					String transactionId = receipt.has(ReceiptValidationHelper.FIELD_TRANSACTION_ID)
							? receipt.get(ReceiptValidationHelper.FIELD_TRANSACTION_ID).getAsString() : null;
					JsonElement expiresDateElement = receipt.get(ReceiptValidationHelper.FIELD_EXPIRES_DATE_MS);
					if (expiresDateElement != null) {
						// It's a subscription
						long expiresDateMs = expiresDateElement.getAsLong();
						if (expiresDateMs > System.currentTimeMillis()) {
							//Subscription is valid (active)
							addStringField(subscriptionObj, receipt, ReceiptValidationHelper.FIELD_ORIGINAL_TRANSACTION_ID);
							if (transactionId != null) { // Ensure transactionId exists before adding
								subscriptionObj.put(ReceiptValidationHelper.FIELD_TRANSACTION_ID, transactionId);
							}
							subscriptionObj.put("expiration_date", Long.toString(expiresDateMs));
							activeSubscriptions.add(subscriptionObj);
						}
					} else {
						// It's an in-app purchase
						inAppArray.add(productId); // Add only productId to the original array
						Map<String, String> inAppDetailedObj = new HashMap<>(); // Create object for the new detailed array
						inAppDetailedObj.put(ReceiptValidationHelper.FIELD_PRODUCT_ID, productId);
						if (transactionId != null) {
							inAppDetailedObj.put(ReceiptValidationHelper.FIELD_TRANSACTION_ID, transactionId);
						}
						// Optionally add other relevant fields like original_transaction_id or purchase_date_ms if needed
						addStringField(inAppDetailedObj, receipt, ReceiptValidationHelper.FIELD_ORIGINAL_TRANSACTION_ID);
						addStringField(inAppDetailedObj, receipt, ReceiptValidationHelper.FIELD_PURCHASE_DATE_MS);
						inAppsDetailedArray.add(inAppDetailedObj);
					}
				}
				result.put("in_apps", inAppArray); // Keep the original array
				result.put("in_apps_detailed", inAppsDetailedArray); // Add the new detailed array
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

	private String addStringField(Map<String, String> purchaseObj, JsonObject receipt, String FIELD) {
		String val = "";
		if (receipt.has(FIELD)) {
			val = receipt.get(FIELD).getAsString();
			purchaseObj.put(FIELD, val);
		}
		return val;
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
