package net.osmand.server.controllers.pub;

import com.google.gson.Gson;
import net.osmand.PlatformUtil;
import net.osmand.purchases.FastSpringHelper;
import net.osmand.server.PurchasesDataLoader;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.EmailSenderService;
import net.osmand.server.api.services.UserSubscriptionService;
import org.apache.commons.logging.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

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
	EmailSenderService emailSender;

	@Autowired
	protected Gson gson;

	@Autowired
	protected PurchasesDataLoader purchasesDataLoader;

	private static final Log LOGGER = PlatformUtil.getLog(FastSpringController.class);


	@Transactional
	@PostMapping("/order-completed")
	public ResponseEntity<String> handleOrderCompletedEvent(@RequestBody FastSpringOrderCompletedRequest request) {
		for (FastSpringOrderCompletedRequest.Event event : request.events) {
			if ("order.completed".equals(event.type)) {
				FastSpringOrderCompletedRequest.Data data = event.data;
				String email = data.tags.userEmail;
				CloudUsersRepository.CloudUser user = usersRepository.findByEmailIgnoreCase(email);
				if (user != null) {
					List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> purchases = new ArrayList<>();
					List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions = new ArrayList<>();
					String orderId = data.order;
					int userId = user.id;
					boolean sendOsmAndAndSpecialGiftEmail = false;
					for (FastSpringOrderCompletedRequest.Item item : data.items) {
						String sku = item.sku;
						LOGGER.info(String.format("FastSpring: hook recorded for user %s %d with orderId: %s, sku: %s, purchaseToken: %s", email, userId, orderId, sku, data.reference));
						if (FastSpringHelper.productSkuMap.contains(sku)) {
							// Handle product purchase
							List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> existingInApps = deviceInAppPurchasesRepository.findByOrderId(orderId);

							if (existingInApps != null && !existingInApps.isEmpty()) {
								LOGGER.error("FastSpring: Purchase already recorded");
								return ResponseEntity.badRequest().body("FastSpring: Purchase already recorded");
							}

							if (sku.contains("osmand_pro_xv")) {
								sendOsmAndAndSpecialGiftEmail = true;
							}

							DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase iap = new DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase();
							iap.orderId = orderId;
							iap.sku = sku;
							iap.purchaseToken = data.reference;
							iap.purchaseTime = new Date(event.created);
							iap.timestamp = new Date();
							iap.userId = userId;
							iap.valid = true;

							purchases.add(iap);
							LOGGER.info(String.format("FastSpring: InApp recorded for user %s purchaseToken: %s", email, data.reference));
						} else if (FastSpringHelper.subscriptionSkuMap.contains(sku)) {
							// Handle subscription purchase
							List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> existingSubscriptions = deviceSubscriptionsRepository.findByOrderId(orderId);

							if (existingSubscriptions != null && !existingSubscriptions.isEmpty()) {
								LOGGER.error("FastSpring: Subscription already recorded");
								return ResponseEntity.badRequest().body("FastSpring: Subscription already recorded " + orderId + " " + sku);
							} else {
								DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription = new DeviceSubscriptionsRepository.SupporterDeviceSubscription();
								subscription.orderId = orderId;
								subscription.sku = sku;
								subscription.purchaseToken = data.reference;
								subscription.timestamp = new Date();
								subscription.userId = userId;
								subscription.valid = true;

								setInitialSubscriptionDates(subscription, sku);

								subscriptions.add(subscription);
								LOGGER.info(String.format("FastSpring: Subscription recorded for user %s purchaseToken: %s", email, data.reference));
							}
						} else {
							LOGGER.error("FastSpring: Unknown product " + sku);
							return ResponseEntity.badRequest().body("FastSpring: Unknown product " + sku);
						}
					}
					purchases.forEach(purchase -> deviceInAppPurchasesRepository.saveAndFlush(purchase));
					subscriptions.forEach(subscription -> deviceSubscriptionsRepository.saveAndFlush(subscription));
					
					userSubService.verifyAndRefreshProOrderId(user);

					if (sendOsmAndAndSpecialGiftEmail) {
						LOGGER.info("FastSpring: Sending special gift email to " + email + " for orderId: " + data.order + ", purchaseToken: " + data.reference);
						emailSender.sendOsmAndSpecialGiftEmail(email);
					}
				} else {
					LOGGER.error("FastSpring: User not found for email " + email + " orderId: " + data.order + ", purchaseToken: " + data.reference);
				}
			}
		}
		return ResponseEntity.ok("OK");
	}

	/**
	 * Set initial starttime and expiretime for FastSpring subscription based on SKU metadata.
	 * This allows the subscription to be active immediately without waiting for the first validation (12 hours).
	 * The dates will be updated with actual values from FastSpring API during the first validation (after 15 minutes).
	 */
	private void setInitialSubscriptionDates(DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription, String sku) {
		subscription.starttime = subscription.timestamp;
		PurchasesDataLoader.Subscription skuData = purchasesDataLoader.getSubscriptions().get(sku);
		if (skuData != null) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(subscription.starttime);
			if ("month".equals(skuData.durationUnit())) {
				cal.add(Calendar.MONTH, skuData.duration());
			} else if ("year".equals(skuData.durationUnit())) {
				cal.add(Calendar.YEAR, skuData.duration());
			}
			subscription.expiretime = cal.getTime();
			subscription.autorenewing = true; // assume autorenew by default
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
			public Tags tags;
			public List<Item> items;
		}

		public static class Customer {
			public String email;
		}

		public static class Tags {
			public String userEmail;
		}

		public static class Item {
			public String sku;
		}
	}

}
