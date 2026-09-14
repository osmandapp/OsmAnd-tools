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
	private static final String ORDER_ANNUAL = "ORDER_ID_TEST000000000";
	private static final String SUBSCRIPTION = "SUBSCRIPTION_ID_TEST00";

	@Mock
	CloudUsersRepository usersRepository;
	@Mock
	DeviceSubscriptionsRepository subs;
	@Spy
	UserSubscriptionService userSubService;
	@InjectMocks
	FastSpringController controller;

	private SupporterDeviceSubscription handle(String webhook, String apiResponse, String sku, String orderId, boolean recorded,
	                                           Function<FastSpringWebhookRequest, ResponseEntity<String>> endpoint) throws IOException {
		SupporterDeviceSubscription s = new SupporterDeviceSubscription();
		s.sku = sku;
		s.orderId = orderId;
		s.valid = true;
		s.autorenewing = true;
		if (recorded) {
			when(subs.findByOrderIdAndSku(orderId, sku)).thenReturn(List.of(s));
		}
		FastSpringWebhookRequest request = FsJson.read(webhook, FastSpringWebhookRequest.class);
		try (MockedStatic<FastSpringHelper> fs = mockStatic(FastSpringHelper.class)) {
			fs.when(() -> FastSpringHelper.getSubscription(SUBSCRIPTION))
					.thenReturn(FsJson.read(apiResponse, FastSpringSubscription.class));
			assertEquals(recorded ? 200 : 202, endpoint.apply(request).getStatusCode().value());
		}
		return s;
	}

	private SupporterDeviceSubscription canceled(boolean recorded) throws IOException {
		return handle("subscription-canceled.json", "subscriptions-get-canceled.json", SKU_MONTHLY, ORDER_MONTHLY, recorded,
				controller::handleSubscriptionCanceledEvent);
	}

	// the hook payload has no order id, the record is found by initialOrderId of the API response
	@Test
	public void canceledStopsAutorenewAndKeepsSubscriptionUntilDeactivationDate() throws IOException {
		SupporterDeviceSubscription s = canceled(true);
		assertTrue("canceled subscription is active on FastSpring until deactivationDate, valid must stay true", s.valid);
		assertFalse("canceled subscription must not autorenew", s.autorenewing);
		assertEquals("expiretime must be FastSpring deactivationDate (2026-10-07)", 1791331200000L, s.expiretime.getTime());
		assertNotNull(s.checktime);
		verify(subs).saveAndFlush(s);
	}

	// the hook may arrive before order.completed is recorded: reject so that FastSpring retries
	@Test
	public void canceledOfUnknownOrderIsRejectedForRetry() throws IOException {
		canceled(false);
		verify(subs, never()).saveAndFlush(any());
	}

	@Test
	public void deactivatedRevokesSubscription() throws IOException {
		SupporterDeviceSubscription s = handle("subscription-deactivated.json", "subscriptions-get-deactivated.json",
				SKU_ANNUAL, ORDER_ANNUAL, true, controller::handleSubscriptionDeactivatedEvent);
		assertFalse("deactivated subscription must be invalid", s.valid);
		assertFalse(s.autorenewing);
		assertEquals("expired", s.kind);
		assertNotNull(s.checktime);
		verify(subs).saveAndFlush(s);
	}
}
