package net.osmand.server.controllers.pub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import net.osmand.PlatformUtil;
import net.osmand.purchases.FastSpringHelper;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.UserSubscriptionService;
import org.apache.commons.logging.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.*;

@Controller
@RequestMapping("/fs")
public class FastSpringController {

	@Autowired
	protected CloudUsersRepository usersRepository;

	@Autowired
	DeviceInAppPurchasesRepository deviceInAppPurchasesRepository;

	@Autowired
	DeviceSubscriptionsRepository deviceSubscriptionsRepository;

	@Autowired
	protected UserSubscriptionService userSubService;

	@Autowired
	protected ObjectMapper objectMapper;

	@Autowired
	protected Gson gson;

	private static final Log LOGGER = PlatformUtil.getLog(FastSpringController.class);

	private static final String DEFAULT_COUNTRY = "UA"; // Default country for pricing if not specified

	@Transactional
	@PostMapping("/order-completed")
	public ResponseEntity<String> handleOrderCompletedEvent(@RequestBody FastSpringOrderCompletedRequest request) {
		for (FastSpringOrderCompletedRequest.Event event : request.events) {
			if ("order.completed".equals(event.type)) {
				FastSpringOrderCompletedRequest.Data data = event.data;
				String email = data.customer.email;
				CloudUsersRepository.CloudUser user = usersRepository.findByEmailIgnoreCase(email);

				if (user != null) {
					List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> purchases = new ArrayList<>();
					List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions = new ArrayList<>();
					String orderId = data.order;
					int userId = user.id;
					for (FastSpringOrderCompletedRequest.Item item : data.items) {
						String sku = item.sku;
						if (FastSpringHelper.productSkuMap.contains(sku)) {
							// Handle product purchase
							List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> existingInApps = deviceInAppPurchasesRepository.findByOrderId(orderId);

							if (existingInApps != null && !existingInApps.isEmpty()) {
								LOGGER.error("FastSpring: Purchase already recorded");
								return ResponseEntity.badRequest().body("FastSpring: Purchase already recorded");
							}

							DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase iap = new DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase();
							iap.orderId = orderId;
							iap.sku = sku;
							iap.platform = FastSpringHelper.FASTSPRING_PLATFORM;
							iap.purchaseToken = data.reference;
							iap.purchaseTime = new Date(event.created);
							iap.timestamp = new Date();
							iap.userId = userId;
							iap.valid = true;

							purchases.add(iap);
						} else if (FastSpringHelper.subscriptionSkuMap.contains(sku)) {
							// Handle subscription purchase
							List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> existingSubscriptions = deviceSubscriptionsRepository.findByOrderId(orderId);

							if (existingSubscriptions != null && !existingSubscriptions.isEmpty()) {
								return ResponseEntity.badRequest().body("FastSpring: Subscription already recorded " + orderId + " " + sku);
							} else {
								DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription = new DeviceSubscriptionsRepository.SupporterDeviceSubscription();
								subscription.orderId = orderId;
								subscription.sku = sku;
								subscription.purchaseToken = data.reference;
								subscription.timestamp = new Date();
								subscription.userId = userId;
								subscription.valid = true;

								subscriptions.add(subscription);
							}
						} else {
							LOGGER.error("FastSpring: Unknown product " + sku);
							return ResponseEntity.badRequest().body("FastSpring: Unknown product " + sku);
						}
					}
					purchases.forEach(purchase -> deviceInAppPurchasesRepository.saveAndFlush(purchase));
					subscriptions.forEach(subscription -> deviceSubscriptionsRepository.saveAndFlush(subscription));

					userSubService.verifyAndRefreshProOrderId(user);
				}
			}
		}
		return ResponseEntity.ok("OK");
	}

	@GetMapping("/products/price")
	public ResponseEntity<String> getPrices(@RequestParam String country) {
		if (!country.matches("^[A-Z]+$")) {
			return ResponseEntity
					.status(HttpStatus.BAD_REQUEST)
					.body("Invalid country code format. Use uppercase letters only.");
		}
		try {
			HttpURLConnection connection = FastSpringHelper.openConnection("/products/price?country=" + country);
			connection.setRequestMethod("GET");
			connection.setRequestProperty("Accept", "application/json");

			int status = connection.getResponseCode();
			if (status != HttpURLConnection.HTTP_OK) {
				return ResponseEntity.status(HttpStatus.BAD_GATEWAY).build();
			}

			try (InputStream is = connection.getInputStream()) {
				JsonNode root = objectMapper.readTree(is);
				JsonNode productsNode = root.path("products");
				List<Map<String, String>> result = new ArrayList<>();

				if (productsNode.isArray()) {
					for (JsonNode productNode : productsNode) {
						String name = productNode.path("product").asText();
						JsonNode pricingNode = productNode.path("pricing");
						JsonNode regionNode = pricingNode.path(country);
						if (regionNode.isMissingNode()) {
							LOGGER.error("FastSpring: No pricing information available for country " + country);
							country = DEFAULT_COUNTRY;
							regionNode = pricingNode.path(country);
						}

						String oldPrice = regionNode.path("price").asText();
						String newPrice = oldPrice;

						String display = regionNode.path("display").asText();
						String newPriceDisplay = display;

						JsonNode discountNode = regionNode.path("quantityDiscount");
						if (discountNode.fieldNames().hasNext()) {
							JsonNode firstTier = discountNode.elements().next();
							if (firstTier.has("unitPrice")) {
								newPrice = firstTier.path("unitPrice").asText();
								newPriceDisplay = firstTier.path("unitPriceDisplay").asText();
							}
						}

						result.add(Map.of(
								"fsName", name,
								"oldPrice", oldPrice,
								"newPrice", newPrice,
								"display", display,
								"newPriceDisplay", newPriceDisplay
						));
					}
				}
				return ResponseEntity.ok(gson.toJson(Collections.singletonMap("prices", result)));
			}

		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error fetching prices: " + e.getMessage());
		}
	}


	public static class FastSpringOrderCompletedRequest {

		public List<Event> events;

		public static class Event {
			public String type;
			public Long created; // purchaseTime
			public Data data;
		}

		public static class Data {
			public String order; // orderId
			public String reference; // purchaseToken
			public Customer customer;
			public List<Item> items;
		}

		public static class Customer {
			public String email;
		}

		public static class Item {
			public String sku;
		}
	}

}
