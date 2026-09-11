package net.osmand.server.fs;

import static net.osmand.server.fs.FastSpringSubscriptionsGetTest.json;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import net.osmand.purchases.FastSpringHelper;
import net.osmand.purchases.FastSpringHelper.FastSpringSubscription;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscription;
import net.osmand.server.api.services.UserSubscriptionService;
import net.osmand.server.controllers.pub.FastSpringController;
import net.osmand.server.controllers.pub.FastSpringController.FastSpringWebhookRequest;

// return.created webhooks: src/test/resources/fs/return-created*.json
@RunWith(MockitoJUnitRunner.class)
public class FastSpringReturnCreatedTest {

	private static final String SKU = "net.osmand.fastspring.subscription.pro.annual";
	private static final String ORDER = "ORDER_ID_TEST000000000";

	@Mock
	CloudUsersRepository usersRepository;
	@Mock
	DeviceInAppPurchasesRepository inApps;
	@Mock
	DeviceSubscriptionsRepository subs;
	@Mock
	UserSubscriptionService userSubService;
	@InjectMocks
	FastSpringController controller;

	private SupporterDeviceSubscription refund(String subscriptionResponse) throws IOException {
		SupporterDeviceSubscription s = new SupporterDeviceSubscription();
		s.sku = SKU;
		s.orderId = ORDER;
		s.valid = true;
		s.autorenewing = true;
		when(subs.findByOrderIdAndSku(ORDER, SKU)).thenReturn(List.of(s));
		FastSpringWebhookRequest request = json("return-created.json", FastSpringWebhookRequest.class);
		try (MockedStatic<FastSpringHelper> fs = mockStatic(FastSpringHelper.class)) {
			fs.when(() -> FastSpringHelper.getSubscriptionByOrderIdAndSku(ORDER, SKU))
					.thenReturn(json(subscriptionResponse, FastSpringSubscription.class));
			assertEquals(200, controller.handleRefundEvent(request).getStatusCode().value());
		}
		verify(subs).saveAndFlush(s);
		return s;
	}

	@Test
	public void refundWithCancelRelatedSubscriptionsRevokes() throws IOException {
		SupporterDeviceSubscription s = refund("subscriptions-get-deactivated.json");
		assertFalse("subscription deactivated on FastSpring must be revoked", s.valid);
		assertEquals("refund", s.kind);
		assertFalse(s.autorenewing);
		assertNotNull(s.checktime);
	}

	@Test
	public void refundWithoutCancellationKeepsCanceledSubscription() throws IOException {
		SupporterDeviceSubscription s = refund("subscriptions-get-canceled.json");
		assertTrue("subscription still active on FastSpring must stay valid", s.valid);
		assertNull(s.kind);
		assertFalse(s.autorenewing);
		assertNotNull(s.checktime);
	}

	// refund may arrive before order.completed is recorded: reject so that FastSpring retries
	@Test
	public void refundOfUnknownOrderIsRejectedForRetry() throws IOException {
		FastSpringWebhookRequest request = json("return-created.json", FastSpringWebhookRequest.class);
		assertEquals(202, controller.handleRefundEvent(request).getStatusCode().value());
		verify(subs, never()).saveAndFlush(any());
	}

	// in-app: no subscription on FastSpring side, revoked by the hook itself
	@Test
	public void inAppRefundRevokes() throws IOException {
		SupporterDeviceInAppPurchase iap = new SupporterDeviceInAppPurchase();
		iap.sku = "net.osmand.fastspring.inapp.maps.plus";
		iap.orderId = "ORDER_ID_INAPP_TEST000";
		iap.valid = true;
		when(inApps.findByOrderIdAndSku(iap.orderId, iap.sku)).thenReturn(List.of(iap));
		FastSpringWebhookRequest request = json("return-created-inapp.json", FastSpringWebhookRequest.class);
		assertEquals(200, controller.handleRefundEvent(request).getStatusCode().value());
		assertFalse(iap.valid);
		assertNotNull(iap.checktime);
		verify(inApps).saveAndFlush(iap);
	}
}
