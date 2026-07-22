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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
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

	@Autowired
	protected PlatformTransactionManager transactionManager;

	private static final Log LOGGER = PlatformUtil.getLog(FastSpringController.class);
	private static final long DAY = 24L * 60 * 60 * 1000;
	private static final int EVENTS_LOOKBACK_DAYS = 30;

	private static final String EVENT_ORDER_COMPLETED = "order.completed";
	private static final String EVENT_RETURN_CREATED = "return.created";
	private static final String EVENT_CHARGEBACK_CREATED = "chargeback.created";
	private static final Set<String> HANDLED_EVENTS = Set.of(EVENT_ORDER_COMPLETED, EVENT_RETURN_CREATED, EVENT_CHARGEBACK_CREATED);


	@Transactional
	@PostMapping("/order-completed")
	public ResponseEntity<String> handleOrderCompletedEvent(@RequestBody FastSpringOrderCompletedRequest request) {
		for (FastSpringOrderCompletedRequest.Event event : request.events) {
			if (EVENT_ORDER_COMPLETED.equals(event.type)) {
				ResponseEntity<String> error = handleOrderCompletedEvent(event);
				if (error != null) {
					return error;
				}
			}
		}
		return ResponseEntity.ok("OK");
	}

	private ResponseEntity<String> handleOrderCompletedEvent(FastSpringOrderCompletedRequest.Event event) {
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

		return null;
	}

	@Transactional
	@PostMapping("/refund")
	public ResponseEntity<String> handleRefundEvent(@RequestBody FastSpringOrderCompletedRequest request) {
		for (FastSpringOrderCompletedRequest.Event event : request.events) {
			if (HANDLED_EVENTS.contains(event.type)) {
				dispatchFastSpringEvent(event);
			}
		}
		return ResponseEntity.ok("OK");
	}

	private void handleReturnCreatedEvent(FastSpringOrderCompletedRequest.Event event) {
		FastSpringOrderCompletedRequest.Data data = event.data;
		if (data == null || data.original == null || data.original.id == null) {
			LOGGER.error("FastSpring: return.created event without original order id, skipping");
			return;
		}
		String orderId = data.original.id;
		if (data.items == null || data.items.isEmpty()) {
			LOGGER.error("FastSpring: return.created event for orderId " + orderId + " has no items, skipping");
			return;
		}
		Set<Integer> affectedUserIds = new HashSet<>();
		for (FastSpringOrderCompletedRequest.Item item : data.items) {
			String sku = item.sku;
			if (sku == null) {
				continue;
			}
			revokePurchases(deviceInAppPurchasesRepository.findByOrderIdAndSku(orderId, sku),
					deviceSubscriptionsRepository.findByOrderIdAndSku(orderId, sku), orderId, affectedUserIds);
		}
		refreshAffectedUsers(affectedUserIds);
	}

	private void handleChargebackCreatedEvent(FastSpringOrderCompletedRequest.Event event) {
		FastSpringOrderCompletedRequest.Data data = event.data;
		if (data == null || data.order == null) {
			LOGGER.error("FastSpring: chargeback.created event without order id, skipping");
			return;
		}
		String orderId = data.order;
		Set<Integer> affectedUserIds = new HashSet<>();
		revokePurchases(deviceInAppPurchasesRepository.findByOrderId(orderId),
				deviceSubscriptionsRepository.findByOrderId(orderId), orderId, affectedUserIds);
		refreshAffectedUsers(affectedUserIds);
	}

	// https://developer.fastspring.com/reference/processed-and-unprocessed-webhook-events
	@Scheduled(fixedRate = DAY)
	public void processMissedFastSpringEvents() {
		try {
			TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
			while (true) {
				String json = FastSpringHelper.getUnprocessedEvents(EVENTS_LOOKBACK_DAYS);
				if (json == null) {
					return;
				}
				FastSpringOrderCompletedRequest resp = gson.fromJson(json, FastSpringOrderCompletedRequest.class);
				if (resp == null || resp.events == null || resp.events.isEmpty()) {
					return;
				}
				int handled = 0;
				for (FastSpringOrderCompletedRequest.Event event : resp.events) {
					if (!HANDLED_EVENTS.contains(event.type)) {
						continue;
					}
					try {
						txTemplate.executeWithoutResult(status -> dispatchFastSpringEvent(event));
						if (event.id != null && FastSpringHelper.markEventProcessed(event.id)) {
							handled++;
						}
					} catch (Exception e) {
						LOGGER.error("FastSpring: failed to process missed event " + event.id
								+ " (" + event.type + "): " + e.getMessage(), e);
					}
				}
				if (handled == 0) {
					return;
				}
			}
		} catch (IOException e) {
			LOGGER.error("FastSpring missed events check failed: " + e.getMessage(), e);
		}
	}

	private void dispatchFastSpringEvent(FastSpringOrderCompletedRequest.Event event) {
		if (EVENT_RETURN_CREATED.equals(event.type)) {
			// https://developer.fastspring.com/reference/returncreated
			handleReturnCreatedEvent(event);
		} else if (EVENT_CHARGEBACK_CREATED.equals(event.type)) {
			// https://developer.fastspring.com/reference/order-chargeback
			handleChargebackCreatedEvent(event);
		} else if (EVENT_ORDER_COMPLETED.equals(event.type)) {
			// https://developer.fastspring.com/reference/ordercompleted
			handleOrderCompletedEvent(event);
		}
	}

	private void revokePurchases(List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> iaps,
	                             List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subs, String orderId, Set<Integer> affectedUserIds) {
		Date now = new Date();
		for (DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase iap : iaps) {
			iap.valid = false;
			iap.checktime = now;
			deviceInAppPurchasesRepository.saveAndFlush(iap);
			if (iap.userId != null) {
				affectedUserIds.add(iap.userId);
			}
			LOGGER.info(String.format("FastSpring: in-app revoked for orderId: %s, sku: %s", orderId, iap.sku));
		}
		for (DeviceSubscriptionsRepository.SupporterDeviceSubscription sub : subs) {
			sub.valid = false;
			sub.autorenewing = false;
			sub.expiretime = now; // expire immediately
			sub.checktime = now;
			deviceSubscriptionsRepository.saveAndFlush(sub);
			if (sub.userId != null) {
				affectedUserIds.add(sub.userId);
			}
			LOGGER.info(String.format("FastSpring: subscription revoked for orderId: %s, sku: %s", orderId, sub.sku));
		}
	}

	private void refreshAffectedUsers(Set<Integer> affectedUserIds) {
		for (Integer userId : affectedUserIds) {
			CloudUsersRepository.CloudUser user = usersRepository.findById(userId);
			if (user != null) {
				userSubService.verifyAndRefreshProOrderId(user);
			}
		}
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
			public String id;
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
			public OriginalOrder original; // present on return.created events (refund)
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

		public static class OriginalOrder {
			public String id;
			public String order;
			public String reference;
		}
	}

}
