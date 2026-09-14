package net.osmand.server.fs;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.ResponseEntity;

import net.osmand.purchases.FastSpringHelper;
import net.osmand.purchases.FastSpringHelper.FastSpringSubscription;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscription;
import net.osmand.server.api.services.UserSubscriptionService;
import net.osmand.server.controllers.pub.FastSpringController;
import net.osmand.server.controllers.pub.FastSpringController.FastSpringWebhookRequest;

// subscription.* webhooks: src/test/resources/fs/subscription-*.json, taken from the docs, no event recorded yet
@RunWith(MockitoJUnitRunner.class)
public class FastSpringSubscriptionEventsTest {

	private static final String SKU_MONTHLY = "net.osmand.fastspring.subscription.pro.monthly";
	private static final String ORDER_MONTHLY = "MP_ORDER_ID_TEST000000";
	private static final String SKU_ANNUAL = "net.osmand.fastspring.subscription.pro.annual";
	private static final String ORDER_ANNUAL_DEACTIVATED = "ORDER_ID_TEST000000000";
	private static final String ORDER_ANNUAL_ACTIVE = "ORDER_ID_SUB_TEST00000";
	private static final String SUBSCRIPTION_CANCELED = "SUBSCRIPTION_ID_TEST00";
	private static final String SUBSCRIPTION_ACTIVE = "SUBSCRIPTION_ID_TEST01";

	@Mock
	CloudUsersRepository usersRepository;
	@Mock
	DeviceSubscriptionsRepository subs;
	@Spy
	UserSubscriptionService userSubService;
	@InjectMocks
	FastSpringController controller;

	private static SupporterDeviceSubscription record(String sku, String orderId) {
		SupporterDeviceSubscription s = new SupporterDeviceSubscription();
		s.sku = sku;
		s.orderId = orderId;
		s.valid = true;
		s.autorenewing = true;
		return s;
	}

	private void handle(SupporterDeviceSubscription s, String webhook, String apiResponse, String subscriptionId, boolean recorded,
	                    Function<FastSpringWebhookRequest, ResponseEntity<String>> endpoint) throws IOException {
		if (recorded) {
			when(subs.findByOrderIdAndSku(s.orderId, s.sku)).thenReturn(List.of(s));
		}
		FastSpringWebhookRequest request = FsJson.read(webhook, FastSpringWebhookRequest.class);
		try (MockedStatic<FastSpringHelper> fs = mockStatic(FastSpringHelper.class)) {
			fs.when(() -> FastSpringHelper.getSubscription(subscriptionId))
					.thenReturn(FsJson.read(apiResponse, FastSpringSubscription.class));
			assertEquals(recorded ? 200 : 202, endpoint.apply(request).getStatusCode().value());
		}
	}

	private SupporterDeviceSubscription canceled(SupporterDeviceSubscription s, boolean recorded) throws IOException {
		handle(s, "subscription-canceled.json", "subscriptions-get-canceled.json", SUBSCRIPTION_CANCELED, recorded,
				controller::handleSubscriptionCanceledEvent);
		return s;
	}

	// the hook payload has no order id, the record is found by initialOrderId of the API response
	@Test
	public void canceledStopsAutorenewAndKeepsSubscriptionUntilDeactivationDate() throws IOException {
		SupporterDeviceSubscription s = canceled(record(SKU_MONTHLY, ORDER_MONTHLY), true);
		assertTrue("canceled subscription is active on FastSpring until deactivationDate, valid must stay true", s.valid);
		assertFalse("canceled subscription must not autorenew", s.autorenewing);
		assertEquals("expiretime must be FastSpring deactivationDate (2026-10-07)", 1791331200000L, s.expiretime.getTime());
		assertNotNull(s.checktime);
		verify(subs).saveAndFlush(s);
	}

	// FastSpring keeps a charged back subscription active until it deactivates, the record must stay revoked
	@Test
	public void canceledDoesNotRestoreChargedBackSubscription() throws IOException {
		SupporterDeviceSubscription s = record(SKU_MONTHLY, ORDER_MONTHLY);
		s.valid = false;
		s.kind = UserSubscriptionService.KIND_CHARGEBACK;
		canceled(s, true);
		assertFalse("charged back subscription must not become valid again", s.valid);
		assertEquals(UserSubscriptionService.KIND_CHARGEBACK, s.kind);
	}

	// the hook may arrive before order.completed is recorded: reject so that FastSpring retries
	@Test
	public void canceledOfUnknownOrderIsRejectedForRetry() throws IOException {
		canceled(record(SKU_MONTHLY, ORDER_MONTHLY), false);
		verify(subs, never()).saveAndFlush(any());
	}

	@Test
	public void deactivatedRevokesSubscription() throws IOException {
		SupporterDeviceSubscription s = record(SKU_ANNUAL, ORDER_ANNUAL_DEACTIVATED);
		handle(s, "subscription-deactivated.json", "subscriptions-get-deactivated.json", SUBSCRIPTION_CANCELED, true,
				controller::handleSubscriptionDeactivatedEvent);
		assertFalse("deactivated subscription must be invalid", s.valid);
		assertFalse(s.autorenewing);
		assertEquals("expired", s.kind);
		assertEquals("expiretime must be FastSpring deactivationDate (2026-09-08)", 1788912000000L, s.expiretime.getTime());
		assertNotNull(s.checktime);
		verify(subs).saveAndFlush(s);
	}

	// a rebill is a new order, the existing record is updated instead of recording the new order id
	@Test
	public void chargeCompletedMovesExpireTimeToNextBillingDate() throws IOException {
		SupporterDeviceSubscription s = record(SKU_ANNUAL, ORDER_ANNUAL_ACTIVE);
		handle(s, "subscription-charge-completed.json", "subscriptions-get-active.json", SUBSCRIPTION_ACTIVE, true,
				controller::handleSubscriptionChargeCompletedEvent);
		assertEquals("expiretime must be FastSpring next billing date (2027-09-11)", 1820620800000L, s.expiretime.getTime());
		assertTrue(s.autorenewing);
		assertEquals("the rebill order id must not be recorded", ORDER_ANNUAL_ACTIVE, s.orderId);
		assertNotNull(s.checktime);
		verify(subs).saveAndFlush(s);
		verify(subs, never()).findByOrderIdAndSku(eq("MP_ORDER_ID_REBILL000"), any());
	}
}
