package net.osmand.purchases;

import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;


public class FastSpringHelper {

	public static final String FASTSPRING_PLATFORM = "fastspring";

	public static final List<String> productMap = List.of("osmand-maps");

	public static final List<String> subscriptionMap = List.of("osmand-pro-annual");

	private static final String API_BASE = "https://api.fastspring.com";
	protected static final Log LOG = LogFactory.getLog(FastSpringHelper.class);

	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			LOG.warn("No subscription ID provided");
			return;
		}
		String orderId = args[0];
		String sku = args[1];
		String type = args[2];

		if (type.equals("-subscription")) {
			FastSpringSubscription sub = getSubscriptionByOrderIdAndSku(orderId, sku);
			if (sub == null) {
				LOG.warn("Failed to get subscription with orderId: " + orderId);
				return;
			}
			LOG.info(String.format("Subscription[id=%s, sku=%s, product=%s, active=%s, autoRenew=%s, begin=%s, nextChargeDate=%s]",
					sub.id, sub.sku, sub.product, sub.active, sub.autoRenew, sub.begin, sub.nextChargeDate));
		} else if (type.equals("-inapp")) {
			FastSpringPurchase inApp = getInAppPurchaseByOrderIdAndSku(orderId, sku);
			if (inApp == null) {
				LOG.warn("Failed to get in-app purchase with orderId: " + orderId);
				return;
			}
			LOG.info(String.format("InAppPurchase[sku=%s, product=%s, purchaseTime=%s, completed=%s, valid=%s]",
					inApp.sku, inApp.product, inApp.purchaseTime, inApp.completed, inApp.isValid()));
		} else {
			LOG.warn("Unknown type: " + type);
		}
	}


	public static FastSpringSubscription getSubscription(String subscriptionId) throws IOException {
		HttpURLConnection connection = openConnection("/subscriptions/" + subscriptionId);
		try (InputStream is = connection.getInputStream();
		     InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
			if (connection.getResponseCode() != 200) {
				LOG.warn("Failed to get FastSpring subscription: " +
						connection.getResponseCode() + " " + connection.getResponseMessage());
				return null;
			}
			FastSpringSubscription subscription = new Gson().fromJson(reader, FastSpringSubscription.class);
			if (subscription == null) {
				LOG.warn("Failed to get FastSpring subscription: " + connection.getResponseCode() + " " + connection.getResponseMessage());
				return null;
			}

			return subscription;
		}
	}

	public static FastSpringSubscription getSubscriptionByOrderIdAndSku(String orderId, String sku) throws IOException {
		FastSpringOrder order = getOrder(orderId);
		if (order == null) {
			return null;
		}
		for (FastSpringOrder.Item item : order.items) {
			if (sku.equals(item.sku)) {
				return getSubscription(item.subscription);
			}
		}
		return null;
	}

	public static FastSpringPurchase getInAppPurchaseByOrderIdAndSku(String orderId, String sku) throws IOException {
		FastSpringOrder order = getOrder(orderId);
		if (order == null) {
			return null;
		}
		for (FastSpringOrder.Item item : order.items) {
			if (sku.equals(item.sku)) {
				Long purchaseTime = order.changed;
				Boolean completed = order.completed;
				return new FastSpringPurchase(item.sku, item.product, purchaseTime, completed);
			}
		}
		return null;
	}

	private static FastSpringOrder getOrder(String orderId) throws IOException {
		HttpURLConnection connection = openConnection("/orders/" + orderId);
		try (InputStream is = connection.getInputStream();
		     InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
			if (connection.getResponseCode() != 200) {
				LOG.warn("Failed to get FastSpring order: " +
						connection.getResponseCode() + " " + connection.getResponseMessage());
				return null;
			}
			FastSpringOrder order = new Gson().fromJson(reader, FastSpringOrder.class);
			if (order == null) {
				LOG.warn("Failed to get FastSpring order: " + connection.getResponseCode() + " " + connection.getResponseMessage());
				return null;
			}

			return order;
		}
	}


	private static HttpURLConnection openConnection(String path) throws IOException {
		String username = System.getenv("FASTSPRING_USERNAME");
		String password = System.getenv("FASTSPRING_PASSWORD");

		if (username == null || password == null) {
			throw new IllegalStateException("Missing FASTSPRING_USERNAME or FASTSPRING_PASSWORD environment variable");
		}

		String auth = username + ":" + password;
		String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));

		URL url = new URL(API_BASE + path);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestMethod("GET");
		connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
		connection.setRequestProperty("Accept", "application/json");

		return connection;
	}

	public static class FastSpringOrder {
		public String id;
		public Long changed; //purchaseTime
		public Boolean completed;
		public List<Item> items;

		public static class Item {
			public String sku;
			public String product;
			public String subscription; //subscriptionId
		}
	}

	public static class FastSpringSubscription {
		public String id;
		public Boolean active;
		public String sku;
		public Long begin; //purchaseTime
		public Long nextChargeDate; //expiretime
		public String product;
		public Boolean autoRenew;
	}

	public static class FastSpringPurchase {
		public String sku;
		public String product;
		public Long purchaseTime;
		public Boolean completed;

		FastSpringPurchase(String sku, String product, Long purchaseTime, Boolean completed) {
			this.sku = sku;
			this.product = product;
			this.purchaseTime = purchaseTime;
			this.completed = completed;
		}

		public boolean isValid() {
			return this.completed;
		}
	}
}
