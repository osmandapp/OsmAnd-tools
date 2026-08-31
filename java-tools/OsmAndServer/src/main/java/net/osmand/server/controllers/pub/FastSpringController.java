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

	// FastSpring subscription states (https://developer.fastspring.com/reference/retrieve-a-subscription)
	private static final String SUBSCRIPTION_STATE_CANCELED = "canceled";
	private static final String SUBSCRIPTION_STATE_DEACTIVATED = "deactivated";

	// values for the "kind" column, same convention as UpdateSubscription.deleteSubscription (expired/invalid/gone)
	private static final String KIND_REFUND = "refund";
	private static final String KIND_CHARGEBACK = "chargeback";

	private static final String SKU_OSMAND_PRO_XV = "osmand_pro_xv";


	// https://developer.fastspring.com/reference/webhooks-overview
	@Transactional
	@PostMapping("/order-completed")
	public ResponseEntity<String> handleOrderCompletedEvent(@RequestBody FastSpringWebhookRequest request) {
		if (request == null || request.events == null) {
			return ResponseEntity.internalServerError().body("FastSpring: empty request");
		}
		return processEventsBatch(request.events, Set.of(EVENT_ORDER_COMPLETED));
	}

	private ResponseEntity<String> handleOrderCompletedEvent(FastSpringWebhookRequest.Event event) {
		FastSpringWebhookRequest.Data data = event.data;
		if (data == null || data.tags == null || data.tags.userEmail == null || data.items == null) {
			LOGGER.error("FastSpring: order.completed event " + event.id + " without user email or items, skipping");
			return null;
		}
		String email = data.tags.userEmail;
		CloudUsersRepository.CloudUser user = usersRepository.findByEmailIgnoreCase(email);
		if (user != null) {
			List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> purchases = new ArrayList<>();
			List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions = new ArrayList<>();
			String orderId = data.order;
			int userId = user.id;
			boolean sendOsmAndAndSpecialGiftEmail = false;
			for (FastSpringWebhookRequest.Item item : data.items) {
				String sku = item.sku;
				LOGGER.info(String.format("FastSpring: hook recorded for user %s %d with orderId: %s, sku: %s, purchaseToken: %s", EmailSenderService.shorten(email), userId, orderId, sku, data.reference));
				if (FastSpringHelper.productSkuMap.contains(sku)) {
					// Handle product purchase
					List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> existingInApps = deviceInAppPurchasesRepository.findByOrderId(orderId);

					if (existingInApps != null && !existingInApps.isEmpty()) {
						LOGGER.info("FastSpring: Purchase already recorded for orderId " + orderId + ", skipping");
						return null;
					}

					if (sku.contains(SKU_OSMAND_PRO_XV)) {
						sendOsmAndAndSpecialGiftEmail = true;
					}

					DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase iap = new DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase();
					iap.orderId = orderId;
					iap.sku = sku;
					iap.purchaseToken = data.reference;
					iap.purchaseTime = event.created != null ? new Date(event.created) : new Date();
					iap.timestamp = new Date();
					iap.userId = userId;
					iap.valid = true;

					purchases.add(iap);
					LOGGER.info(String.format("FastSpring: InApp recorded for user %s purchaseToken: %s", EmailSenderService.shorten(email), data.reference));
				} else if (FastSpringHelper.subscriptionSkuMap.contains(sku)) {
					// Handle subscription purchase
					List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> existingSubscriptions = deviceSubscriptionsRepository.findByOrderId(orderId);

					if (existingSubscriptions != null && !existingSubscriptions.isEmpty()) {
						LOGGER.info("FastSpring: Subscription already recorded for orderId " + orderId + " " + sku + ", skipping");
						return null;
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
						LOGGER.info(String.format("FastSpring: Subscription recorded for user %s purchaseToken: %s", EmailSenderService.shorten(email), data.reference));
					}
				} else {
					LOGGER.error("FastSpring: Unknown product " + sku);
					return ResponseEntity.internalServerError().body("FastSpring: Unknown product " + sku);
				}
			}
			purchases.forEach(purchase -> deviceInAppPurchasesRepository.saveAndFlush(purchase));
			subscriptions.forEach(subscription -> deviceSubscriptionsRepository.saveAndFlush(subscription));

			userSubService.verifyAndRefreshProOrderId(user);

			if (sendOsmAndAndSpecialGiftEmail) {
				LOGGER.info("FastSpring: Sending special gift email to " + EmailSenderService.shorten(email) + " for orderId: " + data.order + ", purchaseToken: " + data.reference);
				emailSender.sendOsmAndSpecialGiftEmail(email);
			}
		} else {
			LOGGER.error("FastSpring: User not found for email " + EmailSenderService.shorten(email) + " orderId: " + data.order + ", purchaseToken: " + data.reference);
		}

		return null;
	}

	// https://developer.fastspring.com/reference/webhooks-overview
	@Transactional
	@PostMapping("/refund")
	public ResponseEntity<String> handleRefundEvent(@RequestBody FastSpringWebhookRequest request) {
		if (request == null || request.events == null) {
			return ResponseEntity.internalServerError().body("FastSpring: empty request");
		}
		return processEventsBatch(request.events, HANDLED_EVENTS);
	}

	// https://developer.fastspring.com/reference/processed-and-unprocessed-webhook-events
	// 200 acknowledges the whole batch; on partial failure return 202 with the ids of the processed events
	// (one per line) so that FastSpring retries only the failed ones.
	private ResponseEntity<String> processEventsBatch(List<FastSpringWebhookRequest.Event> events, Set<String> handledTypes) {
		List<String> processedIds = new ArrayList<>();
		boolean anyFailed = false;
		for (FastSpringWebhookRequest.Event event : events) {
			if (handledTypes.contains(event.type)) {
				ResponseEntity<String> error = dispatchFastSpringEvent(event);
				if (error != null) {
					anyFailed = true;
					LOGGER.error("FastSpring: event " + event.id + " (" + event.type + ") failed: " + error.getBody());
					continue;
				}
			}
			if (event.id != null) {
				processedIds.add(event.id);
			}
		}
		if (!anyFailed) {
			return ResponseEntity.ok("OK");
		}
		return ResponseEntity.accepted().body(String.join("\n", processedIds));
	}

	private ResponseEntity<String> handleReturnCreatedEvent(FastSpringWebhookRequest.Event event) {
		FastSpringWebhookRequest.Data data = event.data;
		if (data == null || data.original == null || data.original.id == null) {
			LOGGER.error("FastSpring: return.created event without original order id, skipping");
			return null;
		}
		String orderId = data.original.id;
		if (data.items == null || data.items.isEmpty()) {
			LOGGER.error("FastSpring: return.created event for orderId " + orderId + " has no items, skipping");
			return null;
		}
		Set<Integer> affectedUserIds = new HashSet<>();
		boolean matchedAny = false;
		for (FastSpringWebhookRequest.Item item : data.items) {
			String sku = item.sku;
			if (sku == null) {
				continue;
			}
			List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> iaps = deviceInAppPurchasesRepository.findByOrderIdAndSku(orderId, sku);
			if (!iaps.isEmpty()) {
				matchedAny = true;
				revokeInAppPurchases(iaps, orderId, affectedUserIds);
			}
			List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subs = deviceSubscriptionsRepository.findByOrderIdAndSku(orderId, sku);
			if (!subs.isEmpty()) {
				matchedAny = true;
				ResponseEntity<String> error = revokeSubscriptionsIfDeactivated(subs, orderId, sku, affectedUserIds);
				if (error != null) {
					return error;
				}
			}
		}
		if (!matchedAny) {
			// Return an error so that the event is retried later: the refund may arrive before the original order is recorded
			return ResponseEntity.internalServerError().body("FastSpring: nothing to revoke for orderId " + orderId);
		}
		refreshAffectedUsers(affectedUserIds);
		return null;
	}

	// A refund does not always cancel the subscription ("Cancel Related Subscriptions" checkbox in the refund dialog),
	// and return.created does not carry that choice, so check the subscription state via API:
	// deactivated -> revoke now, canceled -> stop autorenew only, active -> keep. https://developer.fastspring.com/docs/refund-an-order
	private ResponseEntity<String> revokeSubscriptionsIfDeactivated(List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subs,
	                                                                String orderId, String sku, Set<Integer> affectedUserIds) {
		FastSpringHelper.FastSpringSubscription fsSub;
		try {
			fsSub = FastSpringHelper.getSubscriptionByOrderIdAndSku(orderId, sku);
		} catch (Exception e) {
			LOGGER.error("FastSpring: failed to check subscription state for orderId " + orderId + ", sku " + sku + ": " + e.getMessage(), e);
			return ResponseEntity.internalServerError().body("FastSpring: failed to check subscription state for orderId " + orderId);
		}
		if (fsSub == null) {
			LOGGER.error("FastSpring: subscription not found for refunded orderId " + orderId + ", sku " + sku);
			return ResponseEntity.internalServerError().body("FastSpring: subscription not found for refunded orderId " + orderId);
		}
		if (Boolean.TRUE.equals(fsSub.active) && !SUBSCRIPTION_STATE_DEACTIVATED.equals(fsSub.state)) {
			if (SUBSCRIPTION_STATE_CANCELED.equals(fsSub.state)) {
				Date now = new Date();
				for (DeviceSubscriptionsRepository.SupporterDeviceSubscription sub : subs) {
					sub.autorenewing = false;
					sub.checktime = now;
					deviceSubscriptionsRepository.saveAndFlush(sub);
					LOGGER.info(String.format("FastSpring: subscription canceled after refund, active until period end, orderId: %s, sku: %s", orderId, sub.sku));
				}
			} else {
				LOGGER.info(String.format("FastSpring: refund without subscription cancellation, subscription stays active, orderId: %s, sku: %s", orderId, sku));
			}
			return null;
		}
		revokeSubscriptions(subs, orderId, affectedUserIds, KIND_REFUND);
		return null;
	}

	private ResponseEntity<String> handleChargebackCreatedEvent(FastSpringWebhookRequest.Event event) {
		FastSpringWebhookRequest.Data data = event.data;
		if (data == null || data.order == null) {
			LOGGER.error("FastSpring: chargeback.created event without order id, skipping");
			return null;
		}
		String orderId = data.order;
		Set<Integer> affectedUserIds = new HashSet<>();
		boolean revokedAny = revokePurchases(deviceInAppPurchasesRepository.findByOrderId(orderId),
				deviceSubscriptionsRepository.findByOrderId(orderId), orderId, affectedUserIds, KIND_CHARGEBACK);
		if (!revokedAny) {
			return ResponseEntity.internalServerError().body("FastSpring: nothing to revoke for orderId " + orderId);
		}
		refreshAffectedUsers(affectedUserIds);
		return null;
	}

	// https://developer.fastspring.com/reference/processed-and-unprocessed-webhook-events
	@Scheduled(fixedRate = DAY)
	public void processMissedFastSpringEvents() {
		if (!FastSpringHelper.isConfigured()) {
			LOGGER.info("FastSpring: missed events check skipped, credentials are not configured");
			return;
		}
		LOGGER.info("FastSpring: missed events check started (last " + EVENTS_LOOKBACK_DAYS + " days)");
		try {
			String json = FastSpringHelper.getUnprocessedEvents(EVENTS_LOOKBACK_DAYS);
			if (json == null) {
				LOGGER.warn("FastSpring: missed events check finished, unprocessed events are not available");
				return;
			}
			FastSpringWebhookRequest resp = gson.fromJson(json, FastSpringWebhookRequest.class);
			if (resp == null || resp.events == null || resp.events.isEmpty()) {
				LOGGER.info("FastSpring: missed events check finished, no unprocessed events");
				return;
			}
			LOGGER.info("FastSpring: " + resp.events.size() + " unprocessed events to check");
			resp.events.sort(Comparator.comparingLong(e -> e.created != null ? e.created : 0L));
			TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
			int handled = 0;
			int skipped = 0;
			int failed = 0;
			for (FastSpringWebhookRequest.Event event : resp.events) {
				if (event.id == null) {
					continue;
				}
				try {
					boolean handledType = HANDLED_EVENTS.contains(event.type);
					if (handledType) {
						ResponseEntity<String> error = txTemplate.execute(status -> dispatchFastSpringEvent(event));
						if (error != null) {
							failed++;
							LOGGER.error("FastSpring: missed event " + event.id + " (" + event.type + ") failed: " + error.getBody());
							continue;
						}
					}
					FastSpringHelper.markEventProcessed(event.id);
					if (handledType) {
						handled++;
						LOGGER.info("FastSpring: missed event " + event.id + " (" + event.type + ") processed");
					} else {
						skipped++;
					}
				} catch (Exception e) {
					failed++;
					LOGGER.error("FastSpring: failed to process missed event " + event.id
							+ " (" + event.type + "): " + e.getMessage(), e);
				}
			}
			LOGGER.info(String.format("FastSpring: missed events check finished, processed: %d, skipped (not handled type): %d, failed: %d",
					handled, skipped, failed));
		} catch (IOException e) {
			LOGGER.error("FastSpring missed events check failed: " + e.getMessage(), e);
		}
	}

	private ResponseEntity<String> dispatchFastSpringEvent(FastSpringWebhookRequest.Event event) {
		if (EVENT_RETURN_CREATED.equals(event.type)) {
			// https://developer.fastspring.com/reference/returncreated
			return handleReturnCreatedEvent(event);
		} else if (EVENT_CHARGEBACK_CREATED.equals(event.type)) {
			// https://developer.fastspring.com/reference/order-chargeback
			return handleChargebackCreatedEvent(event);
		} else if (EVENT_ORDER_COMPLETED.equals(event.type)) {
			// https://developer.fastspring.com/reference/ordercompleted
			return handleOrderCompletedEvent(event);
		}
		return null;
	}

	private boolean revokePurchases(List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> iaps,
	                                List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subs, String orderId, Set<Integer> affectedUserIds, String kind) {
		if (iaps.isEmpty() && subs.isEmpty()) {
			LOGGER.warn("FastSpring: nothing to revoke for orderId " + orderId + ", no matching purchases found");
			return false;
		}
		revokeInAppPurchases(iaps, orderId, affectedUserIds);
		revokeSubscriptions(subs, orderId, affectedUserIds, kind);
		return true;
	}

	private void revokeInAppPurchases(List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> iaps, String orderId, Set<Integer> affectedUserIds) {
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
	}

	private void revokeSubscriptions(List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subs, String orderId, Set<Integer> affectedUserIds, String kind) {
		Date now = new Date();
		for (DeviceSubscriptionsRepository.SupporterDeviceSubscription sub : subs) {
			sub.valid = false;
			sub.kind = kind;
			sub.autorenewing = false;
			sub.checktime = now;
			deviceSubscriptionsRepository.saveAndFlush(sub);
			if (sub.userId != null) {
				affectedUserIds.add(sub.userId);
			}
			LOGGER.info(String.format("FastSpring: subscription revoked (%s) for orderId: %s, sku: %s", kind, orderId, sub.sku));
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

	public static class FastSpringWebhookRequest {

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
