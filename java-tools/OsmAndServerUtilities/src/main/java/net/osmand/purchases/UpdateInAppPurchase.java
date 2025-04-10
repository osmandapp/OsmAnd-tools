package net.osmand.purchases;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.model.ProductPurchase;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import net.osmand.PlatformUtil;
import net.osmand.purchases.ReceiptValidationHelper.ReceiptResult;
import net.osmand.util.Algorithms;
import org.apache.commons.logging.Log;
import org.json.JSONException;

import java.io.IOException;
import java.io.Serial;
import java.security.GeneralSecurityException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static net.osmand.purchases.AmazonIAPHelper.*;
import static net.osmand.purchases.HuaweiIAPHelper.*;

public class UpdateInAppPurchase {

    private static final Log LOGGER = PlatformUtil.getLog(UpdateInAppPurchase.class);

    // Constants specific to IAP verification
    public static final String GOOGLE_PACKAGE_NAME = "net.osmand.plus";
    public static final String GOOGLE_PACKAGE_NAME_FREE = "net.osmand";
    public static final String PLATFORM_GOOGLE = "google";
    public static final String PLATFORM_APPLE = "apple";
    public static final String PLATFORM_AMAZON = "amazon";
    public static final String PLATFORM_HUAWEI = "huawei";
    public static final String PLATFORM_FASTSPRING = "fastspring";

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
    private final AmazonIAPHelper amazonHelper; // Amazon Helper
    private final HuaweiIAPHelper huaweiHelper; // Huawei Helper
    private final FastSpringHelper fastSpringHelper;

    public enum PurchasePlatform {
        GOOGLE, APPLE, AMAZON, HUAWEI, FASTSPRING, UNKNOWN
    }

    private static class UpdateParams {
        public boolean verifyAll;
        public boolean verbose;
    }

    // Constructor would take repositories and helpers
    public UpdateInAppPurchase(AndroidPublisher publisher, ReceiptValidationHelper receiptHelper, AmazonIAPHelper amazonHelper, HuaweiIAPHelper huaweiHelper, FastSpringHelper fastSpringHelper) {
        this.publisher = publisher;
        this.receiptHelper = receiptHelper;
        this.amazonHelper = amazonHelper;
        this.huaweiHelper = huaweiHelper;
        this.fastSpringHelper = fastSpringHelper;

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
        boolean onlyAmazon = false;
        boolean onlyHuawei = false;
        boolean onlyFastSpring = false;

        for (String arg : args) {
            if ("-verifyall".equals(arg)) up.verifyAll = true;
            else if ("-verbose".equals(arg)) up.verbose = true;
            else if (arg.startsWith("-androidclientsecret=")) androidClientSecretFile = arg.substring("-androidclientsecret=".length());
            else if ("-onlygoogle".equals(arg)) onlyGoogle = true;
            else if ("-onlyapple".equals(arg)) onlyApple = true;
            else if ("-onlyamazon".equals(arg)) onlyAmazon = true;
            else if ("-onlyhuawei".equals(arg)) onlyHuawei = true;
            else if ("-onlyfastspring".equals(arg)) onlyFastSpring = true;
        }

        AndroidPublisher publisher = null;
        // Initialize publisher only if needed
        if (!onlyApple && !onlyAmazon && !onlyHuawei && !onlyFastSpring && !Algorithms.isEmpty(androidClientSecretFile)) {
            publisher = getPublisherApi(androidClientSecretFile);
        } else if (!onlyApple && !onlyAmazon && !onlyHuawei && !onlyFastSpring) {
            System.err.println("Warning: Android client secret not provided. Cannot verify Google IAPs.");
        }

        // Initialize helpers (consider conditional initialization based on flags)
        ReceiptValidationHelper receiptHelper = new ReceiptValidationHelper();
        AmazonIAPHelper amazonHelper = new AmazonIAPHelper();
        HuaweiIAPHelper huaweiHelper = new HuaweiIAPHelper();
        FastSpringHelper fastSpringHelper = new FastSpringHelper();

        Class.forName("org.postgresql.Driver");
        List<PurchaseUpdateException> exceptionsUpdates = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(System.getenv("DB_CONN"),
                System.getenv("DB_USER"), System.getenv("DB_PWD"))) {

            UpdateInAppPurchase updater = new UpdateInAppPurchase(publisher, receiptHelper, amazonHelper, huaweiHelper, fastSpringHelper);
            updater.queryPurchases(conn, up, exceptionsUpdates, onlyGoogle, onlyApple, onlyAmazon, onlyHuawei, onlyFastSpring);
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

    void queryPurchases(Connection conn, UpdateParams pms, List<PurchaseUpdateException> exceptionsUpdates,
                        boolean onlyGoogle, boolean onlyApple, boolean onlyAmazon, boolean onlyHuawei, boolean onlyFastspring) throws SQLException {
        ResultSet rs = null;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(selQuery);
            updStat = conn.prepareStatement(updQuery);
            delStat = conn.prepareStatement(delQuery);
            updCheckStat = conn.prepareStatement(updCheckQuery);

            while (rs.next()) {
                String sku = rs.getString("sku");
                String purchaseToken = rs.getString("purchaseToken"); // Might be purchaseToken, userId, or receipt data depending on platform
                String orderId = rs.getString("orderid"); // Might be orderId, transaction_id, or receiptId depending on platform
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
                } else if (PLATFORM_AMAZON.equalsIgnoreCase(platform)) {
                    purchasePlatform = PurchasePlatform.AMAZON;
                } else if (PLATFORM_HUAWEI.equalsIgnoreCase(platform)) {
                    purchasePlatform = PurchasePlatform.HUAWEI;
                } else if (PLATFORM_FASTSPRING.equalsIgnoreCase(platform)) {
                    purchasePlatform = PurchasePlatform.FASTSPRING;
                }

                // Skip based on platform filter
                if ((onlyGoogle && purchasePlatform != PurchasePlatform.GOOGLE) ||
                        (onlyApple && purchasePlatform != PurchasePlatform.APPLE) ||
                        (onlyAmazon && purchasePlatform != PurchasePlatform.AMAZON) ||
                        (onlyHuawei && purchasePlatform != PurchasePlatform.HUAWEI) ||
                (onlyFastspring && purchasePlatform != PurchasePlatform.FASTSPRING)) {
                    continue;
                }

                if (validBool != null && validBool && !pms.verifyAll) {
                    continue; // Skip check if already valid
                }

                String hiddenOrderId = getHiddenOrderId(orderId);
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
                        // Assuming purchaseToken field stores the receipt data for Apple
                        updated = processAppleInAppPurchase(purchaseToken, sku, orderId, checkTimeTs, currentTime, pms);
                    } else if (purchasePlatform == PurchasePlatform.AMAZON && amazonHelper != null) { // Add Amazon Call
                        // Parameters for Amazon might need adjustment based on what's stored
                        // Assuming orderId = receiptId, purchaseToken = userId for Amazon helper
                        updated = processAmazonInAppPurchase(purchaseToken, sku, orderId, checkTimeTs, currentTime, pms);
                    } else if (purchasePlatform == PurchasePlatform.HUAWEI && huaweiHelper != null) { // Add Huawei Call
                        // Parameters for Huawei might need adjustment
                        // Assuming purchaseToken = purchaseToken, sku = productId
                        updated = processHuaweiInAppPurchase(purchaseToken, sku, orderId, checkTimeTs, currentTime, pms);
                    } else if (purchasePlatform == PurchasePlatform.FASTSPRING && fastSpringHelper != null) {
                        updated = processFastspringInAppPurchase(purchaseToken, sku, orderId, currentTime, pms);
                    } else {
                        // Handle unknown or unconfigured platforms
                        if (purchasePlatform == PurchasePlatform.UNKNOWN) {
                            invalidReason = "Unknown platform: " + platform;
                            markInvalid = true;
                        } else if (purchasePlatform == PurchasePlatform.GOOGLE && publisher == null) {
                            System.err.printf("Skipping Google IAP check - publisher API not configured (Sku: %s, OrderId: %s)%n", sku, hiddenOrderId);
                            updateCheckTimeOnly(orderId, sku, currentTime);
                            updated = true;
                        } else if (purchasePlatform == PurchasePlatform.AMAZON && amazonHelper == null) {
                            System.err.printf("Skipping Amazon IAP check - helper not configured (Sku: %s, OrderId: %s)%n", sku, hiddenOrderId);
                            updateCheckTimeOnly(orderId, sku, currentTime);
                            updated = true;
                        } else if (purchasePlatform == PurchasePlatform.HUAWEI && huaweiHelper == null) {
                            System.err.printf("Skipping Huawei IAP check - helper not configured (Sku: %s, OrderId: %s)%n", sku, hiddenOrderId);
                            updateCheckTimeOnly(orderId, sku, currentTime);
                            updated = true;
                        }
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

    private static String getHiddenOrderId(String orderId) {
        return orderId != null ? (orderId.substring(0, Math.min(orderId.length(), 18)) + "...") : null;
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
            System.out.println("Updated Google IAP: " + sku + " (" + getHiddenOrderId(orderId) + ") - Valid: " + isValid);
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
            System.out.println("Updated Apple IAP: " + sku + " (" + getHiddenOrderId(orderId) + ") - Valid: true");
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

    private boolean processAmazonInAppPurchase(String amazonUserId, String sku, String amazonReceiptId, Timestamp lastCheckTime, long currentTime, UpdateParams pms) throws SQLException, PurchaseUpdateException {
        // Note: Mapped DB purchaseToken -> amazonUserId, DB orderId -> amazonReceiptId
        if (Algorithms.isEmpty(amazonReceiptId) || Algorithms.isEmpty(amazonUserId)) {
            markAsInvalid(amazonReceiptId != null ? amazonReceiptId : "null_receipt", sku, currentTime, "Missing amazonReceiptId or amazonUserId for validation");
            return true;
        }

        AmazonInAppPurchaseData purchase = null;
        boolean shouldMarkInvalid = false;
        String invalidReason = null;

        try {
            // Call the specific IAP validation method in the helper
            purchase = amazonHelper.getAmazonInAppPurchase(amazonReceiptId, amazonUserId);
            if (pms.verbose && purchase != null) {
                System.out.println("Amazon API Result: " + purchase.toString());
            }
        } catch (IOException e) {
            int errorCode = 0;
            if (e instanceof AmazonIOException) {
                errorCode = ((AmazonIOException) e).responseCode;
            }
            if (errorCode == RESPONSE_CODE_TRANSACTION_ERROR || errorCode == RESPONSE_CODE_USER_ID_ERROR) {
                shouldMarkInvalid = true;
                invalidReason = "Purchase invalid/not found via Amazon API (" + errorCode + "): " + e.getMessage();
            } else { // Other IO or internal errors
                System.err.printf("WARN: IOException verifying Amazon IAP %s (Receipt: %s): %s%n", sku, amazonReceiptId, e.getMessage());
                return false; // Let check time update only
            }
        }

        if (shouldMarkInvalid) {
            // Use the original DB orderId (which we assumed is amazonReceiptId here) for marking invalid
            markAsInvalid(amazonReceiptId, sku, currentTime, invalidReason);
            return true;
        }

        if (purchase != null) {
            // --- Update DB based on Amazon API response ---
            int ind = 1;
            updStat.setTimestamp(ind++, new Timestamp(currentTime)); // checktime

            long purchaseTimeMillis = purchase.purchaseDate != null ? purchase.purchaseDate : 0;
            updStat.setTimestamp(ind++, new Timestamp(purchaseTimeMillis)); // purchase_time

            // Determine validity based on the purchase data (e.g., using purchase.isValid())
            boolean isValid = purchase.isValid(); // Use the validity logic from AmazonInAppPurchaseData
            updStat.setBoolean(ind++, isValid); // valid

            // Where clause parameters (use original DB orderId and sku)
            // We assumed DB orderId = amazonReceiptId for this method call.
            updStat.setString(ind++, amazonReceiptId); // DB orderId
            updStat.setString(ind, sku);           // DB sku

            updStat.addBatch();
            changes++;
            if (changes >= BATCH_SIZE) {
                updStat.executeBatch(); changes = 0;
            }
            System.out.println("Updated Amazon IAP: " + sku + " (Receipt: " + getHiddenOrderId(amazonReceiptId) + ") - Valid: " + isValid);
            return true;
        }

        System.err.println("WARN: Reached unexpected state for Amazon IAP " + sku + " (Receipt: " + amazonReceiptId + ")");
        return false;
    }

    private boolean processHuaweiInAppPurchase(String purchaseToken, String sku, String orderId, Timestamp lastCheckTime, long currentTime, UpdateParams pms) throws SQLException, PurchaseUpdateException {
        // Note: Assuming DB purchaseToken = Huawei purchaseToken, DB sku = productId
        if (Algorithms.isEmpty(purchaseToken) || Algorithms.isEmpty(sku)) {
            markAsInvalid(orderId != null ? orderId : "null_order", sku, currentTime, "Missing purchaseToken or productId (sku) for Huawei validation");
            return true;
        }

        HuaweiInAppPurchaseData purchase = null;
        boolean shouldMarkInvalid = false;
        String invalidReason = null;

        try {
            // Call the specific IAP validation method in the helper
            purchase = huaweiHelper.getHuaweiInAppPurchase(purchaseToken, sku);
            if (pms.verbose && purchase != null) {
                System.out.println("Huawei API Result: " + purchase.toString());
            }
        } catch (IOException e) {
            int errorCode = 0;
            if (e instanceof HuaweiJsonResponseException) {
                errorCode = ((HuaweiJsonResponseException) e).responseCode;
            }
            // Check error codes relevant to non-existence or invalidity
            if (errorCode == RESPONSE_CODE_ORDER_NOT_EXIST_ERROR || errorCode == RESPONSE_CODE_USER_CONSUME_ERROR) {
                shouldMarkInvalid = true;
                invalidReason = "Purchase invalid/not found via Huawei API (" + errorCode + "): " + e.getMessage();
            } else { // Other IO or internal errors
                System.err.printf("WARN: IOException verifying Huawei IAP %s (Token: %s): %s%n", sku, purchaseToken, e.getMessage());
                return false; // Let check time update only
            }
        }

        if (shouldMarkInvalid) {
            // Use the original DB orderId (which might be null/different for Huawei) and sku for marking invalid
            markAsInvalid(orderId, sku, currentTime, invalidReason);
            return true;
        }

        if (purchase != null) {
            // --- Update DB based on Huawei API response ---
            int ind = 1;
            updStat.setTimestamp(ind++, new Timestamp(currentTime)); // checktime

            long purchaseTimeMillis = purchase.purchaseTime != null ? purchase.purchaseTime : 0;
            updStat.setTimestamp(ind++, new Timestamp(purchaseTimeMillis)); // purchase_time

            // Determine validity based on purchase data (e.g., using purchase.isValid())
            boolean isValid = purchase.isValid(); // Use the validity logic from HuaweiInAppPurchaseData
            updStat.setBoolean(ind++, isValid); // valid

            // Where clause parameters (use original DB orderId and sku)
            // Need to ensure the combination of orderId and sku uniquely identifies the record in *our* DB.
            // If Huawei IAPs don't provide a unique orderId that we store in the 'orderid' column,
            // the primary key or update logic might need adjustment.
            // For now, assume the DB record is uniquely identified by the passed 'orderId' and 'sku'.
            updStat.setString(ind++, orderId); // DB orderId
            updStat.setString(ind, sku);       // DB sku

            updStat.addBatch();
            changes++;
            if (changes >= BATCH_SIZE) {
                updStat.executeBatch(); changes = 0;
            }
            System.out.println("Updated Huawei IAP: " + sku + " (DB OrderId: " + getHiddenOrderId(orderId) + ", Token: " + purchaseToken + ") - Valid: " + isValid);
            return true;
        }

        System.err.println("WARN: Reached unexpected state for Huawei IAP " + sku + " (Token: " + purchaseToken + ")");
        return false;
    }

    private boolean processFastspringInAppPurchase(String purchaseToken, String sku, String orderId, long currentTime, UpdateParams pms) throws SQLException {
        if (Algorithms.isEmpty(purchaseToken) || Algorithms.isEmpty(sku)) {
            markAsInvalid(orderId != null ? orderId : "null_order", sku, currentTime, "Missing purchaseToken or productId (sku) for Huawei validation");
            return true;
        }

        FastSpringHelper.FastSpringPurchase purchase = null;

        try {
            purchase = FastSpringHelper.getInAppPurchaseByOrderIdAndSku(orderId, sku);
            if (pms.verbose && purchase != null) {
                LOGGER.info("Fastspring API Result: " + purchase);
            }
        } catch (IOException e) {
            LOGGER.error("WARN: IOException verifying Fastspring IAP " + sku + " (OrderId: " + orderId + "): " + e.getMessage());
        }

        if (purchase == null) {
            markAsInvalid(orderId, sku, currentTime, "Purchase not found via Fastspring API");
        } else {
            int ind = 1;
            updStat.setTimestamp(ind++, new Timestamp(currentTime));
            long purchaseTimeMillis = purchase.purchaseTime;
            updStat.setTimestamp(ind++, new Timestamp(purchaseTimeMillis));
            updStat.setBoolean(ind++, purchase.isValid());
            updStat.setString(ind++, orderId);
            updStat.setString(ind, sku);
            updStat.addBatch();
            changes++;
            if (changes >= BATCH_SIZE) {
                updStat.executeBatch();
                changes = 0;
            }
            LOGGER.info("Updated Fastspring IAP: " + sku + " (DB OrderId: " + getHiddenOrderId(orderId) + ", Token: " + purchaseToken + ") - Valid: " + purchase.isValid());
        }
        return true;
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
