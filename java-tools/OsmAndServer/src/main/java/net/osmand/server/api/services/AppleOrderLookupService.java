package net.osmand.server.api.services;

import net.osmand.purchases.AppStoreOrderLookupHelper;
import net.osmand.purchases.AppStoreOrderLookupHelper.AppStoreTransaction;
import net.osmand.purchases.AppStoreOrderLookupHelper.OrderLookupResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Resolves an Apple order id (the id from the receipt email Apple sends to the customer, MXXXXXXXXX)
 * into the purchases we store. Apple purchases are keyed in supporters_device_sub /
 * supporters_device_iap by original_transaction_id, which the customer never sees, so without this
 * lookup an order id from the email cannot be found in order management at all.
 */
@Service
public class AppleOrderLookupService {

	protected static final Log LOG = LogFactory.getLog(AppleOrderLookupService.class);

	private static final int PURCHASES_PER_TRANSACTION = 10;

	private final AppStoreOrderLookupHelper lookupHelper = new AppStoreOrderLookupHelper();

	@Autowired
	private OrderManagementService orderManagementService;

	public static class AppleOrderLookup {
		public String orderId;
		public boolean configured = true;
		public boolean found;
		public String error;
		public List<AppleTransaction> transactions = new ArrayList<>();
		// rows of supporters_device_sub / supporters_device_iap behind the transactions above
		public List<AdminService.Purchase> purchases = new ArrayList<>();
	}

	public static class AppleTransaction {
		// original_transaction_id, stored as orderid for Apple purchases
		public String orderId;
		public String transactionId;
		public String sku;
		public String type;
		public String environment;
		public String storefront;
		public Date purchaseTime;
		public Date expireTime;
		public Date revocationTime;
		// whether we have a row with this orderid
		public boolean stored;
	}

	public boolean isConfigured() {
		return lookupHelper.isConfigured();
	}

	public AppleOrderLookup lookup(String orderId) {
		AppleOrderLookup res = new AppleOrderLookup();
		res.orderId = orderId;
		if (!isConfigured()) {
			res.configured = false;
			res.error = "App Store Server API key is not configured on the server";
			return res;
		}
		OrderLookupResult lookupResult;
		try {
			lookupResult = lookupHelper.lookupOrder(orderId);
		} catch (IOException | RuntimeException e) {
			LOG.error("Apple order lookup failed for " + orderId + ": " + e.getMessage(), e);
			res.error = "Apple order lookup failed: " + e.getMessage();
			return res;
		}
		if (!lookupResult.isValidOrder()) {
			res.error = "Apple does not know order id " + orderId;
			return res;
		}
		res.found = true;
		Set<String> processedOrderIds = new LinkedHashSet<>();
		for (AppStoreTransaction t : lookupResult.transactions) {
			AppleTransaction transaction = new AppleTransaction();
			transaction.orderId = t.originalTransactionId;
			transaction.transactionId = t.transactionId;
			transaction.sku = t.productId;
			transaction.type = t.type;
			transaction.environment = t.environment;
			transaction.storefront = t.storefront;
			transaction.purchaseTime = toDate(t.purchaseDate);
			transaction.expireTime = toDate(t.expiresDate);
			transaction.revocationTime = toDate(t.revocationDate);
			if (transaction.orderId != null && processedOrderIds.add(transaction.orderId)) {
				List<AdminService.Purchase> found = orderManagementService.searchPurchases(transaction.orderId,
						PURCHASES_PER_TRANSACTION);
				for (AdminService.Purchase p : found) {
					if (transaction.orderId.equals(p.orderId)) {
						res.purchases.add(p);
					}
				}
			}
			transaction.stored = transaction.orderId != null
					&& res.purchases.stream().anyMatch(p -> transaction.orderId.equals(p.orderId));
			res.transactions.add(transaction);
		}
		return res;
	}

	private static Date toDate(Long ms) {
		return ms != null && ms > 0 ? new Date(ms) : null;
	}
}
