package net.osmand.server.controllers.pub;

import lombok.Getter;
import lombok.Setter;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.PremiumUsersRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.*;

import static net.osmand.purchases.FastSpringHelper.*;

@Controller
@RequestMapping("/fs")
public class FastSpringController {

	@Autowired
	protected PremiumUsersRepository usersRepository;

	@Autowired
	DeviceInAppPurchasesRepository deviceInAppPurchasesRepository;

	@Autowired
	DeviceSubscriptionsRepository deviceSubscriptionsRepository;

	// Usually, "order-completed" is expected to arrive before "subscription.activated", but the order is not guaranteed, so both are handled independently.

	@PostMapping("/order-completed")
	public ResponseEntity<String> handleOrderCompletedEvent(@RequestBody FastSpringOrderCompletedRequest request) {
		for (FastSpringOrderCompletedRequest.Event event : request.events) {
			if ("order.completed".equals(event.type)) {
				FastSpringOrderCompletedRequest.Data data = event.data;
				String email = data.customer.email;
				PremiumUsersRepository.PremiumUser user = usersRepository.findByEmailIgnoreCase(email);

				if (user != null) {
					List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> purchases = new ArrayList<>();
					List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions = new ArrayList<>();
					for (FastSpringOrderCompletedRequest.Item item : data.items) {
						String prodId = item.product;
						if (productMap.contains(prodId)) {
							// Handle product purchase
							String orderId = data.order;
							List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> existingInApps = deviceInAppPurchasesRepository.findByOrderId(orderId);

							if (existingInApps != null && !existingInApps.isEmpty()) {
								return ResponseEntity.badRequest().body("FastSpring: Purchase already recorded");
							}

							DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase iap = new DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase();
							iap.purchaseToken = data.reference;
							iap.orderId = data.order;
							iap.sku = item.sku;
							iap.platform = FASTSPRING_PLATFORM;
							iap.purchaseTime = new Date(event.created);
							iap.timestamp = new Date();
							iap.userId = user.id;

							purchases.add(iap);
						} else if (subscriptionMap.contains(prodId)) {
							// Handle subscription purchase
							String orderId = data.order;
							List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> existingSubscriptions = deviceSubscriptionsRepository.findByOrderId(orderId);

							if (existingSubscriptions != null && !existingSubscriptions.isEmpty()) {
								if (existingSubscriptions.size() > 1) {
									return ResponseEntity.badRequest().body("FastSpring: Multiple subscriptions found for orderId " + orderId + " and purchaseToken " + data.reference);
								}
								DeviceSubscriptionsRepository.SupporterDeviceSubscription existingSubscription = existingSubscriptions.get(0);

								if (!existingSubscription.purchaseToken.equals(data.reference) || !existingSubscription.orderId.equals(orderId)) {
									return ResponseEntity.badRequest().body("FastSpring: purchaseToken or orderId mismatch " + data.reference + " " + orderId);
								}
								existingSubscription.userId = user.id;

								subscriptions.add(existingSubscription);
							} else {
								DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription = new DeviceSubscriptionsRepository.SupporterDeviceSubscription();
								subscription.purchaseToken = data.reference;
								subscription.orderId = data.order;
								subscription.sku = item.sku;
								subscription.timestamp = new Date();
								subscription.userId = user.id;

								subscriptions.add(subscription);
							}
						} else {
							return ResponseEntity.badRequest().body("FastSpring: Unknown product " + prodId);
						}
					}
					purchases.forEach(purchase -> deviceInAppPurchasesRepository.saveAndFlush(purchase));
					subscriptions.forEach(subscription -> deviceSubscriptionsRepository.saveAndFlush(subscription));
				}

			}
		}
		return ResponseEntity.ok("OK");
	}

	@PostMapping("/subscription-activated")
	public ResponseEntity<String> handleSubscriptionActivatedEvent(@RequestBody FastSpringSubscriptionActivatedRequest request) {
		for (FastSpringSubscriptionActivatedRequest.Event event : request.events) {
			if ("subscription.activated".equals(event.type)) {
				FastSpringSubscriptionActivatedRequest.Data data = event.data;
				if (!subscriptionMap.contains(data.product)) {
					return ResponseEntity.badRequest().body("FastSpring: Unknown subscription " + data.product);
				}
				String orderId = data.initialOrderId;
				String sku = data.sku;
				String purchaseToken = data.initialOrderReference;
				List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> existingSubscriptions = deviceSubscriptionsRepository.findByOrderIdAndPurchaseTokenAndSku(orderId, purchaseToken, sku);

				if (existingSubscriptions == null || existingSubscriptions.isEmpty()) {
					DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription = new DeviceSubscriptionsRepository.SupporterDeviceSubscription();
					subscription.purchaseToken = purchaseToken;
					subscription.orderId = orderId;
					subscription.sku = data.sku;
					subscription.starttime = new Date(data.begin);
					subscription.expiretime = new Date(data.nextChargeDate);
					subscription.timestamp = new Date();

					deviceSubscriptionsRepository.saveAndFlush(subscription);
					return ResponseEntity.ok("OK");
				}

				if (existingSubscriptions.size() > 1) {
					return ResponseEntity.badRequest().body("FastSpring: Multiple subscriptions found for orderId " + orderId + " and purchaseToken " + purchaseToken + " and sku " + sku);
				}

				DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription = existingSubscriptions.get(0);
				subscription.starttime = new Date(data.begin);
				subscription.expiretime = new Date(data.nextChargeDate);

				deviceSubscriptionsRepository.saveAndFlush(subscription);
			}
		}
		return ResponseEntity.ok("OK");
	}


	@Setter
	@Getter
	public static class FastSpringOrderCompletedRequest {

		private List<Event> events;

		@Getter
		@Setter
		public static class Event {
			private String type;
			private Long created; // purchaseTime
			private Data data;
		}

		@Getter
		@Setter
		private static class Data {
			private String reference; // purchaseToken
			private String order; // orderId
			private Customer customer;
			private List<Item> items;
		}

		@Getter
		@Setter
		private static class Customer {
			private String email;
		}

		@Getter
		@Setter
		private static class Item {
			private String product;
			private String sku;
		}
	}


	@Getter
	@Setter
	public static class FastSpringSubscriptionActivatedRequest {

		private List<Event> events;

		@Getter
		@Setter
		public static class Event {
			private String type;
			private Data data;
		}

		@Getter
		@Setter
		private static class Data {
			private String initialOrderId; // orderId
			private String product;
			private String sku;
			private String initialOrderReference; // purchaseToken
			private Long begin; // purchaseTime
			private Long nextChargeDate; // expiretime
		}
	}

}
