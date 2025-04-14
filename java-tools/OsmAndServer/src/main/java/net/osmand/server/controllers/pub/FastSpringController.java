package net.osmand.server.controllers.pub;

import lombok.Getter;
import lombok.Setter;
import net.osmand.PlatformUtil;
import net.osmand.purchases.FastSpringHelper;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.PremiumUsersRepository;
import org.apache.commons.logging.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.*;

@Controller
@RequestMapping("/fs")
public class FastSpringController {

	@Autowired
	protected PremiumUsersRepository usersRepository;

	@Autowired
	DeviceInAppPurchasesRepository deviceInAppPurchasesRepository;

	@Autowired
	DeviceSubscriptionsRepository deviceSubscriptionsRepository;

	private static final Log LOGGER = PlatformUtil.getLog(FastSpringController.class);

	// Usually, "order-completed" is expected to arrive before "subscription.activated", but the order is not guaranteed, so both are handled independently.

	@Transactional
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
							iap.purchaseTime = new Date(event.created);
							iap.timestamp = new Date();
							iap.userId = userId;
							iap.valid = true;

							purchases.add(iap);
						} else if (FastSpringHelper.subscriptionSkuMap.contains(sku)) {
							// Handle subscription purchase
							List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> existingSubscriptions = deviceSubscriptionsRepository.findByOrderId(orderId);

							if (existingSubscriptions != null && !existingSubscriptions.isEmpty()) {
								if (existingSubscriptions.size() > 1) {
									LOGGER.error("FastSpring: Multiple subscriptions found for orderId " + orderId + " and sku " + sku);
									return ResponseEntity.badRequest().body("FastSpring: Multiple subscriptions found for orderId " + orderId + " and sku " + sku);
								}
								DeviceSubscriptionsRepository.SupporterDeviceSubscription existingSubscription = existingSubscriptions.get(0);

								if (!existingSubscription.sku.equals(sku) || !existingSubscription.orderId.equals(orderId)) {
									LOGGER.error("FastSpring: sku or orderId mismatch " + sku + " " + orderId);
									return ResponseEntity.badRequest().body("FastSpring: sku or orderId mismatch " + sku + " " + orderId);
								}
								existingSubscription.userId = userId;
								existingSubscription.valid = true;
								updatePremiumUserOrderId(user.id, orderId, sku);

								subscriptions.add(existingSubscription);
							} else {
								DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription = new DeviceSubscriptionsRepository.SupporterDeviceSubscription();
								subscription.orderId = orderId;
								subscription.sku = sku;
								subscription.timestamp = new Date();
								subscription.userId = userId;

								subscriptions.add(subscription);
							}
						} else {
							LOGGER.error("FastSpring: Unknown product " + sku);
							return ResponseEntity.badRequest().body("FastSpring: Unknown product " + sku);
						}
					}
					purchases.forEach(purchase -> deviceInAppPurchasesRepository.saveAndFlush(purchase));
					subscriptions.forEach(subscription -> deviceSubscriptionsRepository.saveAndFlush(subscription));
				}

			}
		}
		return ResponseEntity.ok("OK");
	}

	@Transactional
	@PostMapping("/subscription-activated")
	public ResponseEntity<String> handleSubscriptionActivatedEvent(@RequestBody FastSpringSubscriptionActivatedRequest request) {
		for (FastSpringSubscriptionActivatedRequest.Event event : request.events) {
			if ("subscription.activated".equals(event.type)) {
				FastSpringSubscriptionActivatedRequest.Data data = event.data;
				String sku = data.sku;
				if (!FastSpringHelper.subscriptionSkuMap.contains(sku)) {
					LOGGER.error("FastSpring: Unknown subscription " + sku);
					return ResponseEntity.badRequest().body("FastSpring: Unknown subscription " + sku);
				}
				String orderId = data.initialOrderId;
				List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> existingSubscriptions = deviceSubscriptionsRepository.findByOrderIdAndSku(orderId, sku);

				if (existingSubscriptions == null || existingSubscriptions.isEmpty()) {
					DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription = new DeviceSubscriptionsRepository.SupporterDeviceSubscription();
					subscription.orderId = orderId;
					subscription.sku = sku;
					subscription.starttime = new Date(data.begin);
					subscription.expiretime = new Date(data.nextChargeDate);
					subscription.timestamp = new Date();

					deviceSubscriptionsRepository.saveAndFlush(subscription);
					return ResponseEntity.ok("OK");
				}

				if (existingSubscriptions.size() > 1) {
					LOGGER.error("FastSpring: Multiple subscriptions found for orderId " + orderId + " and sku " + sku);
					return ResponseEntity.badRequest().body("FastSpring: Multiple subscriptions found for orderId " + orderId + " and sku " + sku);
				}

				DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription = existingSubscriptions.get(0);
				subscription.starttime = new Date(data.begin);
				subscription.expiretime = new Date(data.nextChargeDate);
				subscription.valid = true;
				updatePremiumUserOrderId(subscription.userId, orderId, sku);

				deviceSubscriptionsRepository.saveAndFlush(subscription);
			}
		}
		return ResponseEntity.ok("OK");
	}

	private void updatePremiumUserOrderId(int userId, String orderId, String sku) {
		if (orderId == null) {
			return;
		}
		if (!FastSpringHelper.proSubscriptionSkuMap.contains(sku)) {
			return;
		}
		PremiumUsersRepository.PremiumUser user = usersRepository.findById(userId);
		if (user == null) {
			return;
		}
		if (user.orderid != null && !user.orderid.isEmpty()) {
			List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions = deviceSubscriptionsRepository.findByOrderId(user.orderid);
			if (subscriptions != null && !subscriptions.isEmpty()) {
				DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription = subscriptions.get(0);
				if (subscription != null && Boolean.TRUE.equals(subscription.valid)) {
					return;
				}
			}
		}
		user.orderid = orderId;
		usersRepository.saveAndFlush(user);
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
			private String sku;
			private Long begin; // purchaseTime
			private Long nextChargeDate; // expiretime
		}
	}

}
