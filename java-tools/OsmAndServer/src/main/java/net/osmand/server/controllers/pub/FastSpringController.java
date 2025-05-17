package net.osmand.server.controllers.pub;

import lombok.Getter;
import lombok.Setter;
import net.osmand.PlatformUtil;
import net.osmand.purchases.FastSpringHelper;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.CloudUsersRepository;
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
	protected CloudUsersRepository usersRepository;

	@Autowired
	DeviceInAppPurchasesRepository deviceInAppPurchasesRepository;

	@Autowired
	DeviceSubscriptionsRepository deviceSubscriptionsRepository;

	private static final Log LOGGER = PlatformUtil.getLog(FastSpringController.class);

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
								subscription.timestamp = new Date();
								subscription.userId = userId;
								subscription.valid = true;
								// TODO remove as non necessary
//								updatePremiumUserOrderId(user.id, orderId, sku);

								subscriptions.add(subscription);
							}
						} else {
							LOGGER.error("FastSpring: Unknown product " + sku);
							return ResponseEntity.badRequest().body("FastSpring: Unknown product " + sku);
						}
					}
					purchases.forEach(purchase -> deviceInAppPurchasesRepository.saveAndFlush(purchase));
					subscriptions.forEach(subscription -> deviceSubscriptionsRepository.saveAndFlush(subscription));
					// TODO this code should be used everywhere to update 
//					String errorMsg = userSubService.checkOrderIdPremium(pu.orderid);
//					if (errorMsg != null) {
//						userSubService.updateOrderId(pu);
//					}
				}

			}
		}
		return ResponseEntity.ok("OK");
	}

	private void updatePremiumUserOrderId(int userId, String orderId, String sku) {
		// TODO this code should be deleted
		if (orderId == null || !FastSpringHelper.proSubscriptionSkuMap.contains(sku)) {
			return;
		}
		CloudUsersRepository.CloudUser user = usersRepository.findById(userId);
		if (user == null) {
			return;
		}
		if (user.orderid != null && !user.orderid.isEmpty()) {
			List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions = deviceSubscriptionsRepository.findByOrderId(user.orderid);
			if (subscriptions != null && !subscriptions.isEmpty()) {
				for (DeviceSubscriptionsRepository.SupporterDeviceSubscription s : subscriptions) {
					if (s.sku.equals(sku) && Boolean.TRUE.equals(s.valid)) {
						return;
					}
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

}
