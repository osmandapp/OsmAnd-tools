package net.osmand.purchases;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Client for the App Store Server API "Look Up Order ID" endpoint:
 * https://developer.apple.com/documentation/appstoreserverapi/look-up-order-id
 * <p>
 * Apple emails the customer a receipt that contains an order id (looks like MXXXXXXXXX). That id is
 * not part of the app receipt, so it never reaches supporters_device_sub / supporters_device_iap,
 * where Apple purchases are keyed by original_transaction_id. This helper converts an order id into
 * the transactions behind it, so support can find (or register) the purchase by what the user has.
 * <p>
 * Configuration (App Store Connect -> Users and Access -> Integrations -> In-App Purchase key):
 * <ul>
 * <li>IOS_STORE_API_KEY - base64 body of the .p8 key (PKCS#8, without the BEGIN/END lines)</li>
 * <li>IOS_STORE_API_KEY_ID - key id of that key</li>
 * <li>IOS_STORE_API_ISSUER_ID - issuer id shown above the key list</li>
 * </ul>
 * The lookup endpoint is not available in the sandbox environment, production orders only.
 */
public class AppStoreOrderLookupHelper {

	public static final String LOOKUP_URL = "https://api.storekit.itunes.apple.com/inApps/v1/lookup/";

	// https://developer.apple.com/documentation/appstoreserverapi/orderlookupstatus
	public static final int ORDER_VALID = 0;
	public static final int ORDER_INVALID = 1;

	public static final String ENV_KEY = "IOS_STORE_API_KEY";
	public static final String ENV_KEY_ID = "IOS_STORE_API_KEY_ID";
	public static final String ENV_ISSUER_ID = "IOS_STORE_API_ISSUER_ID";

	private static final String AUDIENCE = "appstoreconnect-v1";
	// Apple rejects tokens valid for more than 60 minutes
	private static final long TOKEN_LIFETIME_SEC = 20 * 60;
	private static final int TIMEOUT_MS = 20000;

	public static class OrderLookupResult {
		public int status;
		public String message;
		public List<AppStoreTransaction> transactions = new ArrayList<>();

		public boolean isValidOrder() {
			return status == ORDER_VALID;
		}
	}

	/**
	 * Decoded JWSTransaction payload:
	 * https://developer.apple.com/documentation/appstoreserverapi/jwstransactiondecodedpayload
	 */
	public static class AppStoreTransaction {
		public String originalTransactionId;
		public String transactionId;
		public String productId;
		public String type;
		public String inAppOwnershipType;
		public String environment;
		public String storefront;
		public Long purchaseDate;
		public Long originalPurchaseDate;
		public Long expiresDate;
		public Long revocationDate;
		public Integer revocationReason;
	}

	public boolean isConfigured() {
		return !isEmpty(System.getenv(ENV_KEY)) && !isEmpty(System.getenv(ENV_KEY_ID))
				&& !isEmpty(System.getenv(ENV_ISSUER_ID));
	}

	/**
	 * @param orderId order id from the customer's Apple receipt email
	 * @return transactions Apple knows for that order id; an unknown order id is reported as
	 * {@link #ORDER_INVALID} instead of an exception
	 */
	public OrderLookupResult lookupOrder(String orderId) throws IOException {
		if (isEmpty(orderId)) {
			throw new IllegalArgumentException("Order id is empty");
		}
		if (!isConfigured()) {
			throw new IllegalStateException(String.format("App Store Server API is not configured (%s, %s, %s)",
					ENV_KEY, ENV_KEY_ID, ENV_ISSUER_ID));
		}
		String token;
		try {
			token = generateToken();
		} catch (Exception e) {
			throw new IOException("Cannot sign App Store Server API token: " + e.getMessage(), e);
		}
		JsonObject response = httpGet(LOOKUP_URL + orderId.trim(), token);
		OrderLookupResult result = new OrderLookupResult();
		JsonElement status = response.get("status");
		result.status = status != null && !status.isJsonNull() ? status.getAsInt() : ORDER_INVALID;
		if (!result.isValidOrder()) {
			result.message = "Apple does not know this order id";
			return result;
		}
		JsonElement signedTransactions = response.get("signedTransactions");
		if (signedTransactions != null && signedTransactions.isJsonArray()) {
			for (JsonElement jws : signedTransactions.getAsJsonArray()) {
				AppStoreTransaction transaction = parseTransaction(decodeJwsPayload(jws.getAsString()));
				if (transaction != null) {
					result.transactions.add(transaction);
				}
			}
		}
		return result;
	}

	private AppStoreTransaction parseTransaction(JsonObject payload) {
		if (payload == null) {
			return null;
		}
		AppStoreTransaction transaction = new AppStoreTransaction();
		transaction.originalTransactionId = getString(payload, "originalTransactionId");
		transaction.transactionId = getString(payload, "transactionId");
		transaction.productId = getString(payload, "productId");
		transaction.type = getString(payload, "type");
		transaction.inAppOwnershipType = getString(payload, "inAppOwnershipType");
		transaction.environment = getString(payload, "environment");
		transaction.storefront = getString(payload, "storefront");
		transaction.purchaseDate = getLong(payload, "purchaseDate");
		transaction.originalPurchaseDate = getLong(payload, "originalPurchaseDate");
		transaction.expiresDate = getLong(payload, "expiresDate");
		transaction.revocationDate = getLong(payload, "revocationDate");
		JsonElement reason = payload.get("revocationReason");
		transaction.revocationReason = reason != null && !reason.isJsonNull() ? reason.getAsInt() : null;
		return transaction;
	}

	/**
	 * Signed payloads are read without verifying the certificate chain: they are received over TLS
	 * from Apple in response to a request signed with our own private key and are only displayed to
	 * admins, never used as a proof of purchase on its own.
	 */
	public static JsonObject decodeJwsPayload(String jws) {
		if (isEmpty(jws)) {
			return null;
		}
		String[] parts = jws.split("\\.");
		if (parts.length != 3) {
			return null;
		}
		String payload = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
		JsonElement parsed = new JsonParser().parse(payload);
		return parsed.isJsonObject() ? parsed.getAsJsonObject() : null;
	}

	private String generateToken() throws Exception {
		String keyId = System.getenv(ENV_KEY_ID);
		String issuerId = System.getenv(ENV_ISSUER_ID);
		long now = System.currentTimeMillis() / 1000L;

		JsonObject header = new JsonObject();
		header.addProperty("alg", "ES256");
		header.addProperty("kid", keyId);
		header.addProperty("typ", "JWT");

		JsonObject payload = new JsonObject();
		payload.addProperty("iss", issuerId);
		payload.addProperty("iat", now);
		payload.addProperty("exp", now + TOKEN_LIFETIME_SEC);
		payload.addProperty("aud", AUDIENCE);
		payload.addProperty("bid", ReceiptValidationHelper.IOS_MAPS_BUNDLE_ID);

		Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
		String signingInput = encoder.encodeToString(header.toString().getBytes(StandardCharsets.UTF_8))
				+ "." + encoder.encodeToString(payload.toString().getBytes(StandardCharsets.UTF_8));

		byte[] pkcs8 = Base64.getDecoder().decode(cleanupKey(System.getenv(ENV_KEY)));
		PrivateKey privateKey = KeyFactory.getInstance("EC").generatePrivate(new PKCS8EncodedKeySpec(pkcs8));
		// P1363 is the (r || s) format required by JWS ES256, unlike the default DER encoding
		Signature ecdsa = Signature.getInstance("SHA256withECDSAinP1363Format");
		ecdsa.initSign(privateKey);
		ecdsa.update(signingInput.getBytes(StandardCharsets.UTF_8));

		return signingInput + "." + encoder.encodeToString(ecdsa.sign());
	}

	// tolerate a .p8 pasted with its PEM header/footer and line breaks
	private static String cleanupKey(String key) {
		return key.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "")
				.replaceAll("\\s", "");
	}

	private JsonObject httpGet(String url, String token) throws IOException {
		HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
		try {
			connection.setRequestMethod("GET");
			connection.setRequestProperty("Authorization", "Bearer " + token);
			connection.setRequestProperty("Accept", "application/json");
			connection.setConnectTimeout(TIMEOUT_MS);
			connection.setReadTimeout(TIMEOUT_MS);

			int code = connection.getResponseCode();
			// 404 with errorCode 4040005 is Apple's answer for an order id it has no transactions for
			InputStream in = code == HttpURLConnection.HTTP_OK ? connection.getInputStream() : connection.getErrorStream();
			String body = in != null ? read(in) : "";
			if (code == HttpURLConnection.HTTP_OK) {
				JsonElement parsed = new JsonParser().parse(body);
				if (!parsed.isJsonObject()) {
					throw new IOException("Unexpected App Store Server API response: " + body);
				}
				return parsed.getAsJsonObject();
			}
			if (code == HttpURLConnection.HTTP_NOT_FOUND) {
				JsonObject notFound = new JsonObject();
				notFound.addProperty("status", ORDER_INVALID);
				return notFound;
			}
			throw new IOException(String.format("App Store Server API error %d: %s", code, body));
		} finally {
			connection.disconnect();
		}
	}

	private static String read(InputStream in) throws IOException {
		StringBuilder builder = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
			String line;
			while ((line = reader.readLine()) != null) {
				builder.append(line);
			}
		}
		return builder.toString();
	}

	private static String getString(JsonObject obj, String field) {
		JsonElement el = obj.get(field);
		return el != null && !el.isJsonNull() ? el.getAsString() : null;
	}

	private static Long getLong(JsonObject obj, String field) {
		JsonElement el = obj.get(field);
		return el != null && !el.isJsonNull() ? el.getAsLong() : null;
	}

	private static boolean isEmpty(String s) {
		return s == null || s.isEmpty();
	}
}
