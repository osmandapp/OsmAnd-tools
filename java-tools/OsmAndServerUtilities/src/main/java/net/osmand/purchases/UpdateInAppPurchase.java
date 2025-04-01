package net.osmand.purchases;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.model.ProductPurchase;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import net.osmand.purchases.ReceiptValidationHelper.ReceiptResult;
import net.osmand.util.Algorithms;
import org.json.JSONException;

import java.io.*;
import java.security.GeneralSecurityException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class UpdateInAppPurchase {

    // Constants specific to IAP verification
    public static final String GOOGLE_PACKAGE_NAME = "net.osmand.plus";
    public static final String GOOGLE_PACKAGE_NAME_FREE = "net.osmand";
    public static final String PLATFORM_GOOGLE = "google";
    public static final String PLATFORM_APPLE = "apple";

    private static final int BATCH_SIZE = 200;
    private static final long HOUR = 1000L * 60 * 60;

    private static final long MINIMUM_WAIT_TO_REVALIDATE = 12 * HOUR;
    private static final long MAX_WAITING_TIME_TO_MAKE_INVALID = TimeUnit.DAYS.toMillis(5); // If pending/invalid for too long

    private int changes = 0;
    private int checkChanges = 0;
    private int deletions = 0;
    private final String selQuery;
    private final String updQuery;
    private final String delQuery;
    private final String updCheckQuery;
    private PreparedStatement updStat;
    private PreparedStatement delStat;
    private PreparedStatement updCheckStat;

    private final AndroidPublisher publisher; // Google API
    private final ReceiptValidationHelper receiptHelper; // Apple API

    public enum PurchasePlatform {
        GOOGLE, APPLE, UNKNOWN
    }

    private static class UpdateParams {
        public boolean verifyAll;
        public boolean verbose;
    }

    // Constructor would take repositories and helpers
    public UpdateInAppPurchase(AndroidPublisher publisher) {
        this.publisher = publisher;
        this.receiptHelper = new ReceiptValidationHelper();

        // --- Define SQL Queries for the `supporters_device_iap` table ---
        // Select purchases that are pending validation or haven't been checked recently
        selQuery = "SELECT sku, purchaseToken, orderid, platform, purchase_time, checktime, valid, userid, supporterid, timestamp "
                + "FROM supporters_device_iap "
                + "WHERE valid IS NULL OR valid = TRUE " // Focus on potentially valid ones first
                // Add condition based on checktime later in the loop
                + "ORDER BY timestamp ASC";

        // Update query for IAP status
        updQuery = "UPDATE supporters_device_iap SET "
                + " checktime = ?, purchase_time = ?, valid = ? "
                + " WHERE orderid = ? AND sku = ?";

        // Query to just update checktime if no other change needed or error occurred
        updCheckQuery = "UPDATE supporters_device_iap SET checktime = ? "
                + " WHERE orderid = ? AND sku = ?";

        // Optional: Query to mark as invalid (instead of deleting)
        delQuery = "UPDATE supporters_device_iap SET valid = false, checktime = ? "
                + " WHERE orderid = ? and sku = ?";
    }


    public static void main(String[] args) throws Exception {
        UpdateParams up = new UpdateParams();
        String androidClientSecretFile = "";
        boolean onlyGoogle = false;
        boolean onlyApple = false;

        for (String arg : args) {
            if ("-verifyall".equals(arg)) up.verifyAll = true;
            else if ("-verbose".equals(arg)) up.verbose = true;
            else if (arg.startsWith("-androidclientsecret=")) androidClientSecretFile = arg.substring("-androidclientsecret=".length());
            else if ("-onlygoogle".equals(arg)) onlyGoogle = true;
            else if ("-onlyapple".equals(arg)) onlyApple = true;
        }

        AndroidPublisher publisher = null;
        if (!onlyApple && !Algorithms.isEmpty(androidClientSecretFile)) {
            publisher = getPublisherApi(androidClientSecretFile);
        } else if (!onlyApple) {
            System.err.println("Warning: Android client secret not provided. Cannot verify Google IAPs.");
        }

        Class.forName("org.postgresql.Driver");
        List<PurchaseUpdateException> exceptionsUpdates = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(System.getenv("DB_CONN"),
                System.getenv("DB_USER"), System.getenv("DB_PWD"))) {

            // Note: In a Spring Boot app, you'd inject the DataSource/JdbcTemplate/Repository
            // For this standalone structure, we use direct JDBC
            UpdateInAppPurchase updater = new UpdateInAppPurchase(publisher); // Pass repo if using Spring context
            updater.queryPurchases(conn, up, exceptionsUpdates, onlyGoogle, onlyApple);
        }

        if (!exceptionsUpdates.isEmpty()) {
            StringBuilder msg = new StringBuilder("Errors during IAP update:\n");
            for (PurchaseUpdateException e : exceptionsUpdates) {
                msg.append(e.orderid).append(" (").append(e.sku).append(") ").append(e.getMessage()).append("\n");
            }
            throw new IllegalStateException(msg.toString());
        } else {
            System.out.println("IAP update process finished successfully.");
        }
    }

    void queryPurchases(Connection conn, UpdateParams pms, List<PurchaseUpdateException> exceptionsUpdates, boolean onlyGoogle, boolean onlyApple) throws SQLException {
        ResultSet rs = null;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(selQuery);
            updStat = conn.prepareStatement(updQuery);
            delStat = conn.prepareStatement(delQuery); // For marking invalid
            updCheckStat = conn.prepareStatement(updCheckQuery);

            while (rs.next()) {
                String sku = rs.getString("sku");
                String purchaseToken = rs.getString("purchaseToken");
                String orderId = rs.getString("orderid");
                String platform = rs.getString("platform");
                Timestamp purchaseTimeTs = rs.getTimestamp("purchase_time");
                Timestamp checkTimeTs = rs.getTimestamp("checktime");
                Boolean validBool = rs.getObject("valid", Boolean.class);
                // Integer userId = rs.getObject("userid", Integer.class);
                // Timestamp recordTimestamp = rs.getTimestamp("timestamp");

                long currentTime = System.currentTimeMillis();
                long delayBetweenChecks = checkTimeTs == null ? MINIMUM_WAIT_TO_REVALIDATE : (currentTime - checkTimeTs.getTime());
                if (delayBetweenChecks < MINIMUM_WAIT_TO_REVALIDATE && !pms.verifyAll) {
                    // in case validate all (ignore minimum waiting time)
                    continue;
                }

                // Determine platform enum
                PurchasePlatform purchasePlatform = PurchasePlatform.UNKNOWN;
                if (PLATFORM_GOOGLE.equalsIgnoreCase(platform)) {
                    purchasePlatform = PurchasePlatform.GOOGLE;
                } else if (PLATFORM_APPLE.equalsIgnoreCase(platform)) {
                    purchasePlatform = PurchasePlatform.APPLE;
                }

                // Skip based on platform filter
                if ((onlyGoogle && purchasePlatform != PurchasePlatform.GOOGLE) || (onlyApple && purchasePlatform != PurchasePlatform.APPLE)) {
                    continue;
                }

                if (validBool != null && validBool && !pms.verifyAll) {
                    continue; // Skip check if already valid
                }

                String hiddenOrderId = orderId != null ? (orderId.substring(0, Math.min(orderId.length(), 18)) + "...") : orderId;
                System.out.printf("Validate IAP (%s, %s): %s - %s (Platform: %s, Valid: %s)%n",
                        sku, hiddenOrderId,
                        purchaseTimeTs == null ? "?" : new Date(purchaseTimeTs.getTime()),
                        checkTimeTs == null ? "Never" : new Date(checkTimeTs.getTime()),
                        platform,
                        validBool);

                try {
                    boolean updated = false;
                    boolean markInvalid = false;
                    String invalidReason = null;

                    if (purchasePlatform == PurchasePlatform.GOOGLE && publisher != null) {
                        updated = processGoogleInAppPurchase(purchaseToken, sku, orderId, checkTimeTs, currentTime, pms);
                    } else if (purchasePlatform == PurchasePlatform.APPLE) {
                        updated = processAppleInAppPurchase(purchaseToken, sku, orderId, checkTimeTs, currentTime, pms);
                    } else {
                        // Mark as invalid or log error if platform is unknown or handler unavailable
                        if (purchasePlatform == PurchasePlatform.UNKNOWN) {
                            invalidReason = "Unknown platform";
                            markInvalid = true;
                        } else if (purchasePlatform == PurchasePlatform.GOOGLE && publisher == null) {
                            System.err.println("Skipping Google IAP check - publisher API not configured.");
                            // Just update check time to avoid constant retries
                            updateCheckTimeOnly(orderId, sku, currentTime);
                            updated = true; // Mark as processed for batching
                        }
                        // else: platform known but no handler (e.g. Huawei/Amazon) - skip or mark invalid? Skip for now.
                    }

                    if (!updated && markInvalid) {
                        markAsInvalid(orderId, sku, currentTime, invalidReason);
                        updated = true; // Mark as processed for batching
                    } else if (!updated && validBool == null && checkTimeTs != null && (currentTime - checkTimeTs.getTime() > MAX_WAITING_TIME_TO_MAKE_INVALID)) {
                        // If validation keeps failing (updated=false) and it's been pending long enough, mark invalid
                        markAsInvalid(orderId, sku, currentTime, "Failed to validate after multiple attempts");
                        updated = true;
                    } else if (!updated) {
                        // If not updated and not marked invalid, just update the check time
                        updateCheckTimeOnly(orderId, sku, currentTime);
                        updated = true; // Mark as processed for batching
                    }

                } catch (PurchaseUpdateException e) {
                    exceptionsUpdates.add(e);
                    // Also update check time for this specific entry to avoid retrying immediately
                    updateCheckTimeOnly(orderId, sku, currentTime);
                }
            } // end while loop

            // Execute remaining batches
            if (changes > 0) {
                updStat.executeBatch();
            }
            if (deletions > 0) {
                delStat.executeBatch();
            }
            if (checkChanges > 0) {
                updCheckStat.executeBatch();
            }

            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } finally {
            // Close resources
            if (rs != null) try {
                rs.close();
            } catch (SQLException e) { /* ignore */ }
            if (stmt != null) try {
                stmt.close();
            } catch (SQLException e) { /* ignore */ }
            if (updStat != null) try {
                updStat.close();
            } catch (SQLException e) { /* ignore */ }
            if (delStat != null) try {
                delStat.close();
            } catch (SQLException e) { /* ignore */ }
            if (updCheckStat != null) try {
                updCheckStat.close();
            } catch (SQLException e) { /* ignore */ }
        }
    }

    private boolean processGoogleInAppPurchase(String purchaseToken, String sku, String orderId, Timestamp lastCheckTime, long currentTime, UpdateParams pms) throws SQLException, PurchaseUpdateException {
        ProductPurchase purchase = null;
        boolean shouldMarkInvalid = false;
        String invalidReason = null;

        try {
            String packageName = sku.contains("_plus") ? GOOGLE_PACKAGE_NAME : GOOGLE_PACKAGE_NAME_FREE;
            purchase = publisher.purchases().products().get(packageName, sku, purchaseToken).execute();

            if (pms.verbose) {
                System.out.println("Google API Result: " + purchase.toPrettyString());
            }

        } catch (IOException e) {
            int errorCode = 0;
            if (e instanceof GoogleJsonResponseException) {
                errorCode = ((GoogleJsonResponseException) e).getStatusCode();
            }
            // Handle specific errors
            if (errorCode == 400 || errorCode == 404 || errorCode == 410) { // Bad request, Not found, Gone (purchase invalid)
                shouldMarkInvalid = true;
                invalidReason = "Purchase not found or invalid via Google API (" + errorCode + "): " + e.getMessage();
            } else {
                // Other IO errors - log and maybe retry later (don't mark invalid yet)
                System.err.printf("WARN: IOException verifying Google IAP %s (%s): %s%n", sku, orderId, e.getMessage());
                // Don't throw PurchaseUpdateException here, let check time update
                return false; // Indicate no successful update occurred
            }
        }

        if (shouldMarkInvalid) {
            markAsInvalid(orderId, sku, currentTime, invalidReason);
            return true; // Update processed (marked invalid)
        }

        if (purchase != null) {
            // --- Update DB based on Google API response ---
            int ind = 1;
            updStat.setTimestamp(ind++, new Timestamp(currentTime)); // checktime

            long purchaseTimeMillis = purchase.getPurchaseTimeMillis() != null ? purchase.getPurchaseTimeMillis() : 0;
            updStat.setTimestamp(ind++, new Timestamp(purchaseTimeMillis)); // purchase_time

            // Determine validity based on purchase state
            // 0: Purchased, 1: Canceled, 2: Pending
            boolean isValid = purchase.getPurchaseState() != null && purchase.getPurchaseState() == 0;
            updStat.setBoolean(ind++, isValid); // valid

            // Where clause parameters
            updStat.setString(ind++, orderId);
            updStat.setString(ind, sku);

            updStat.addBatch();
            changes++;
            if (changes >= BATCH_SIZE) {
                updStat.executeBatch();
                changes = 0;
            }
            System.out.println("Updated Google IAP: " + sku + " (" + orderId + ") - Valid: " + isValid);
            return true; // Update processed
        }

        // Should not reach here if purchase was null due to handled exceptions
        System.err.println("WARN: Reached unexpected state for Google IAP " + sku + " (" + orderId + ")");
        return false; // No update occurred
    }


    private boolean processAppleInAppPurchase(String purchaseToken, String sku, String orderId, Timestamp lastCheckTime, long currentTime, UpdateParams pms) throws SQLException, PurchaseUpdateException {
        // purchaseToken for iOS IAP often refers to the transaction_id sent from the client
        // The actual validation needs the receipt data (stored in purchaseToken field in our DB)

        String receiptData = purchaseToken; // Assuming receipt is stored here for iOS
        if (Algorithms.isEmpty(receiptData)) {
            markAsInvalid(orderId, sku, currentTime, "Missing receipt data for Apple IAP validation");
            return true;
        }

        ReceiptResult validationResult = receiptHelper.loadReceipt(receiptData, false); // Start with production

        // Handle sandbox redirect
        if (validationResult.error == ReceiptValidationHelper.SANDBOX_ERROR_CODE_TEST) {
            validationResult = receiptHelper.loadReceipt(receiptData, true); // Retry with sandbox
        }

        if (pms.verbose && validationResult.response != null) {
            System.out.println("Apple API Result: " + validationResult.response);
        }

        if (!validationResult.result) {
            // Handle specific Apple errors
            if (validationResult.error == ReceiptValidationHelper.USER_GONE || validationResult.error == 21003 || validationResult.error == 21004) { // User gone, auth failed, secret mismatch
                markAsInvalid(orderId, sku, currentTime, "Purchase invalid via Apple API (Error: " + validationResult.error + ")");
                return true;
            } else {
                // Other errors, log and maybe retry later
                System.err.printf("WARN: Error verifying Apple IAP %s (%s): Code %d%n", sku, orderId, validationResult.error);
                // Don't throw, let check time update
                return false;
            }
        }

        // --- Validation successful, find the specific IAP within the receipt ---
        JsonObject responseObj = validationResult.response;
        boolean foundAndValid = false;
        long purchaseTimeMillis = 0;

        // Check both 'in_app' and 'latest_receipt_info' arrays
        JsonArray inAppPurchases = null;
        if (responseObj != null && responseObj.has("receipt") && responseObj.getAsJsonObject("receipt").has("in_app")) {
            inAppPurchases = responseObj.getAsJsonObject("receipt").getAsJsonArray("in_app");
        }
        JsonArray latestReceiptInfo = responseObj != null && responseObj.has("latest_receipt_info")
                ? responseObj.getAsJsonArray("latest_receipt_info") : null;

        List<JsonArray> arraysToSearch = new ArrayList<>();
        if (latestReceiptInfo != null) {
            arraysToSearch.add(latestReceiptInfo); // Prefer latest info
        }
        if (inAppPurchases != null) {
            arraysToSearch.add(inAppPurchases);
        }

        for (JsonArray purchaseArray : arraysToSearch) {
            if (purchaseArray == null) continue;
            for (JsonElement elem : purchaseArray) {
                JsonObject purchase = elem.getAsJsonObject();
                String itemSku = purchase.has("product_id") ? purchase.get("product_id").getAsString() : null;
                String itemTransactionId = purchase.has("transaction_id") ? purchase.get("transaction_id").getAsString() : null;
                // Also check original_transaction_id if needed, but orderId should match transaction_id for the specific purchase event
                // String itemOriginalTransactionId = purchase.has("original_transaction_id") ? purchase.get("original_transaction_id").getAsString() : null;

                if (sku.equals(itemSku) && orderId.equals(itemTransactionId)) {
                    // Found the matching purchase
                    foundAndValid = true; // If it's in the receipt and status was 0, it's valid.
                    if (purchase.has("purchase_date_ms")) {
                        purchaseTimeMillis = purchase.get("purchase_date_ms").getAsLong();
                    }
                    break; // Found the item
                }
            }
            if (foundAndValid) break; // Stop searching if found
        }


        // --- Update DB based on validation result ---
        int ind = 1;
        updStat.setTimestamp(ind++, new Timestamp(currentTime)); // checktime

        if (foundAndValid) {
            updStat.setTimestamp(ind++, new Timestamp(purchaseTimeMillis)); // purchase_time
            updStat.setBoolean(ind++, true); // valid
            System.out.println("Updated Apple IAP: " + sku + " (" + orderId + ") - Valid: true");
        } else {
            // If validation succeeded (status 0) but the specific IAP wasn't found in the arrays
            markAsInvalid(orderId, sku, currentTime, "IAP not found within the validated Apple receipt");
            return true; // Processed (marked invalid)
        }

        // Where clause parameters
        updStat.setString(ind++, orderId);
        updStat.setString(ind, sku);

        updStat.addBatch();
        changes++;
        if (changes >= BATCH_SIZE) {
            updStat.executeBatch();
            changes = 0;
        }
        return true; // Update processed
    }

    private void markAsInvalid(String orderId, String sku, long tm, String reason) throws SQLException {
        if (delStat == null) {
            System.err.println("WARN: delStat is null, cannot mark as invalid: " + sku + " (" + orderId + ")");
            return;
        }
        delStat.setTimestamp(1, new Timestamp(tm));
        delStat.setString(2, orderId);
        delStat.setString(3, sku);
        delStat.addBatch();
        deletions++;
        System.out.printf("!! Marking IAP as invalid: sku=%s, orderId=%s. Reason: %s%n", sku, orderId, reason);
        if (deletions > BATCH_SIZE) {
            delStat.executeBatch();
            deletions = 0;
        }
    }

    private void updateCheckTimeOnly(String orderId, String sku, long tm) throws SQLException {
        if (updCheckStat == null) {
            System.err.println("WARN: updCheckStat is null, cannot update check time: " + sku + " (" + orderId + ")");
            return;
        }
        updCheckStat.setTimestamp(1, new Timestamp(tm));
        updCheckStat.setString(2, orderId);
        updCheckStat.setString(3, sku);
        updCheckStat.addBatch();
        checkChanges++;
        if (checkChanges > BATCH_SIZE) {
            updCheckStat.executeBatch();
            checkChanges = 0;
        }
    }


    // --- Static methods for API setup (copied from UpdateSubscription) ---
    private static HttpRequestInitializer setHttpTimeout(final HttpRequestInitializer requestInitializer) {
        return httpRequest -> {
            requestInitializer.initialize(httpRequest);
            httpRequest.setConnectTimeout((int) TimeUnit.MINUTES.toMillis(2)); // 2 min connect timeout
            httpRequest.setReadTimeout((int) TimeUnit.MINUTES.toMillis(2));    // 2 min read timeout
        };
    }

    public static AndroidPublisher getPublisherApi(String file) throws JSONException, IOException, GeneralSecurityException {
        return UpdateSubscription.getPublisherApi(file);
    }

    // Exception class
    protected static class PurchaseUpdateException extends Exception {
        @Serial
        private static final long serialVersionUID = 4L;

        private final String orderid;
        private final String sku;

        public PurchaseUpdateException(String orderid, String sku, String message) {
            super(message);
            this.orderid = orderid;
            this.sku = sku;
        }

        public String getOrderid() {
            return orderid;
        }

        public String getSku() {
            return sku;
        }
    }
}
