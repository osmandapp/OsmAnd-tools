package net.osmand.server.fs;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

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

	private static final String SKU = "net.osmand.fastspring.subscription.pro.monthly";
	private static final String ORDER = "MP_ORDER_ID_TEST000000";
	private static final String SUBSCRIPTION = "SUBSCRIPTION_ID_TEST00";

	@Mock
	CloudUsersRepository usersRepository;
	@Mock
	DeviceSubscriptionsRepository subs;
	@Spy
	UserSubscriptionService userSubService;
	@InjectMocks
	FastSpringController controller;

	private SupporterDeviceSubscription canceled(boolean recorded) throws IOException {
		SupporterDeviceSubscription s = new SupporterDeviceSubscription();
		s.sku = SKU;
		s.orderId = ORDER;
		s.valid = true;
		s.autorenewing = true;
		if (recorded) {
			when(subs.findByOrderIdAndSku(ORDER, SKU)).thenReturn(List.of(s));
		}
		FastSpringWebhookRequest request = FsJson.read("subscription-canceled.json", FastSpringWebhookRequest.class);
		try (MockedStatic<FastSpringHelper> fs = mockStatic(FastSpringHelper.class)) {
			fs.when(() -> FastSpringHelper.getSubscription(SUBSCRIPTION))
					.thenReturn(FsJson.read("subscriptions-get-canceled.json", FastSpringSubscription.class));
			assertEquals(recorded ? 200 : 202, controller.handleSubscriptionCanceledEvent(request).getStatusCode().value());
		}
		return s;
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
}
