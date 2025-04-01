package net.osmand.purchases;

import net.osmand.util.Algorithms;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class AmazonIAPHelper {

	// The transaction represented by this receiptId is invalid, or no transaction was found for this receiptId.
	public static final int RESPONSE_CODE_TRANSACTION_ERROR = 400;
	// Invalid sharedSecret
	public static final int RESPONSE_CODE_SHARED_SECRET_ERROR = 496;
	// Invalid User ID
	public static final int RESPONSE_CODE_USER_ID_ERROR = 497;
	// There was an Internal Server Error
	public static final int RESPONSE_CODE_INTERNAL_ERROR = 500;

	public static class AmazonSubscription {

		public final JSONObject dataJson;

		/*
		{
		  "autoRenewing": false,
		  "betaProduct": false,
		  "cancelDate": null,
		  "cancelReason": null,
		  "deferredDate": null,
		  "deferredSku": null,
		  "freeTrialEndDate": 1606985788979,
		  "gracePeriodEndDate": 1606985788979,
		  "parentProductId": null,
		  "productId": "com.amazon.subs1",
		  "productType": "SUBSCRIPTION",
		  "purchaseDate": 1604613233106,
		  "quantity": 1,
		  "receiptId": "q1YqVbJSyjH28DGPKChw9c0o8nd3ySststQtzSkrzM8tCk43K6z0d_HOTcwwN8vxCrVV0lEqBmpJzs_VS8xNrMrP0ysuTSo2BAqXKFkZ6SilACUNzQxMzAyNjYyNDQ3MgDKJSlZpiTnFqTpK6UpWJUWlQEYahFELAA",
		  "renewalDate": 1606985788979,
		  "term": "1 Month",
		  "termSku": "com.amazon.subs1_term",
		  "testTransaction": true
		}
		*/

		public final Boolean autoRenewing;
		public final Boolean betaProduct;
		public final Long cancelDate;
		public final Integer cancelReason;
		public final Long freeTrialEndDate;
		public final Long gracePeriodEndDate;
		public final String parentProductId;
		public final String productId;
		public final String productType;
		public final Long purchaseDate;
		public final Integer quantity;
		public final String receiptId;
		public final Long renewalDate;
		public final String term;
		public final String termSku;
		public final Boolean testTransaction;


		public AmazonSubscription(String inappPurchaseData) {
			this.dataJson = new JSONObject(inappPurchaseData);
			autoRenewing = dataJson.has("autoRenewing") ? dataJson.optBoolean("autoRenewing") : null;
			betaProduct = dataJson.has("autoRenewing") ? dataJson.optBoolean("betaProduct") : null;
			cancelDate = dataJson.has("cancelDate") && !dataJson.isNull("cancelDate") ? dataJson.optLong("cancelDate") : null;
			cancelReason = dataJson.has("cancelReason") && !dataJson.isNull("cancelReason") ? dataJson.optInt("cancelReason") : null;
			freeTrialEndDate = dataJson.has("freeTrialEndDate") && !dataJson.isNull("freeTrialEndDate") ? dataJson.optLong("freeTrialEndDate") : null;
			gracePeriodEndDate = dataJson.has("gracePeriodEndDate") && !dataJson.isNull("gracePeriodEndDate") ? dataJson.optLong("gracePeriodEndDate") : null;
			parentProductId = dataJson.optString("parentProductId", null);
			productId = dataJson.optString("productId", null);
			productType = dataJson.optString("productType", null);
			purchaseDate = dataJson.has("purchaseDate") && !dataJson.isNull("purchaseDate") ? dataJson.optLong("purchaseDate") : null;
			quantity = dataJson.has("quantity") && !dataJson.isNull("quantity") ? dataJson.optInt("quantity") : null;
			receiptId = dataJson.optString("receiptId", null);
			renewalDate = dataJson.has("renewalDate") && !dataJson.isNull("renewalDate") ? dataJson.optLong("renewalDate") : null;
			term = dataJson.optString("term", null);
			termSku = dataJson.optString("termSku", null);
			testTransaction = dataJson.has("testTransaction") ? dataJson.optBoolean("testTransaction") : null;
		}

		@Override
		public String toString() {
			return dataJson.toString();
		}
	}

    /**
     * Represents the response from Amazon's IAP (non-subscription) verification.
     * Adjust fields based on actual Amazon RVS V2 API response for consumables/entitlements.
     */
    public static class AmazonInAppPurchaseData {
        public final JSONObject dataJson;

        /* Example structure:
        {
          "betaProduct": false,
          "cancelDate": null, // May be null for non-subs
          "productId": "com.amazon.iap1",
          "productType": "CONSUMABLE", "ENTITLED"
          "purchaseDate": 1604613233106,
          "quantity": 1,
          "receiptId": "q1Yq...", // The identifier for this specific purchase
          "termSku": null, // Likely null for non-subs
          "testTransaction": true
        }
        */

        public final Boolean betaProduct;
        public final Long cancelDate; // Check if applicable for IAP
        public final String productId;
        public final String productType; // e.g., CONSUMABLE, ENTITLED
        public final Long purchaseDate;
        public final Integer quantity;
        public final String receiptId; // This specific purchase receipt
        public final Boolean testTransaction;

        public AmazonInAppPurchaseData(String iapData) {
            this.dataJson = new JSONObject(iapData);
            betaProduct = dataJson.has("betaProduct") ? dataJson.optBoolean("betaProduct") : null;
            cancelDate = dataJson.has("cancelDate") && !dataJson.isNull("cancelDate") ? dataJson.optLong("cancelDate") : null;
            productId = dataJson.optString("productId", null);
            productType = dataJson.optString("productType", null);
            purchaseDate = dataJson.has("purchaseDate") && !dataJson.isNull("purchaseDate") ? dataJson.optLong("purchaseDate") : null;
            quantity = dataJson.has("quantity") && !dataJson.isNull("quantity") ? dataJson.optInt("quantity") : null;
            receiptId = dataJson.optString("receiptId", null);
            testTransaction = dataJson.has("testTransaction") ? dataJson.optBoolean("testTransaction") : null;
        }

        public boolean isValid() {
            return this.purchaseDate != null && this.productId != null;
        }

        @Override
        public String toString() {
            return dataJson.toString();
        }
    }

	public static class AmazonIOException extends IOException {
		private static final long serialVersionUID = 1495071835071404232L;

		public final int responseCode;
		public final String responseMessage;

		public AmazonIOException(int responseCode, String responseMessage) {
			super(responseMessage);
			this.responseCode = responseCode;
			this.responseMessage = responseMessage;
		}
	}

	public AmazonSubscription getAmazonSubscription(String receiptId, String userId) throws IOException {
		String sharedSecret = System.getenv("AMAZON_SHARED_SECRET");
		String response = httpGet("https://appstore-sdk.amazon.com/version/1.0/verifyReceiptId/developer/"
				+ sharedSecret + "/user/" + userId + "/receiptId/" + receiptId);
		if (Algorithms.isEmpty(response)) {
			throw new IOException(String.format("No purchase data returned for receiptId: '%s': %s", receiptId, userId));
		}
		return new AmazonSubscription(response);
	}

    /**
     * Validates a non-subscription In-App Purchase with Amazon.
     * Uses the same endpoint as subscriptions for verifyReceiptId but parses the response differently.
     *
     * @param receiptId The receipt ID of the specific IAP transaction.
     * @param userId    The Amazon User ID associated with the purchase.
     * @return Parsed IAP data.
     * @throws IOException If the request fails or the response indicates an error.
     */
    public AmazonInAppPurchaseData getAmazonInAppPurchase(String receiptId, String userId) throws IOException {
        String sharedSecret = System.getenv("AMAZON_SHARED_SECRET");
        // The verifyReceiptId endpoint is often used for both subs and IAPs,
        // but double-check Amazon RVS v2 docs if a separate endpoint exists/is preferred for IAPs.
        String url = "https://appstore-sdk.amazon.com/version/1.0/verifyReceiptId/developer/"
                + sharedSecret + "/user/" + userId + "/receiptId/" + receiptId;

        String response = httpGet(url); // Uses the same shared httpGet method

        if (Algorithms.isEmpty(response)) {
            throw new IOException(String.format("No IAP data returned for receiptId: '%s', userId: %s", receiptId, userId));
        }
        // Parse response using the new IAP-specific class
        return new AmazonInAppPurchaseData(response);
    }

	private static String httpGet(String httpUrl) throws IOException {
		HttpURLConnection urlConnection = null;
		BufferedReader bufferedReader = null;
		InputStream in = null;
		InputStreamReader inputStreamReader = null;
		try {
			URL url = new URL(httpUrl);
			urlConnection = (HttpURLConnection) url.openConnection();
			urlConnection.setRequestMethod("GET");
			urlConnection.setConnectTimeout(10000);
			urlConnection.setReadTimeout(10000);
			urlConnection.connect();

			// read response
			int responseCode = urlConnection.getResponseCode();
			if (responseCode != 200) {
				String message;
				switch (responseCode) {
					case RESPONSE_CODE_TRANSACTION_ERROR:
						message = "The transaction represented by this receiptId is invalid, or no transaction was found for this receiptId.";
						break;
					case RESPONSE_CODE_SHARED_SECRET_ERROR:
						message = "Invalid sharedSecret";
						break;
					case RESPONSE_CODE_USER_ID_ERROR:
						message = "Invalid User ID";
						break;
					case RESPONSE_CODE_INTERNAL_ERROR:
						message = "There was an Internal Server Error";
						break;
					default:
						message = "Unknown error";
				}
				throw new AmazonIOException(responseCode, message);
			}

			in = urlConnection.getInputStream();
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
