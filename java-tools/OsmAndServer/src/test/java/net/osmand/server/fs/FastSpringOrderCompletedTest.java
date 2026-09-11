package net.osmand.server.fs;

import static net.osmand.server.fs.FastSpringSubscriptionsGetTest.json;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.node.BooleanNode;

import net.osmand.server.PurchasesDataLoader;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscription;
import net.osmand.server.api.services.UserSubscriptionService;
import net.osmand.server.controllers.pub.FastSpringController;
import net.osmand.server.controllers.pub.FastSpringController.FastSpringWebhookRequest;

// order.completed webhooks: src/test/resources/fs/order-completed-*.json
@RunWith(MockitoJUnitRunner.class)
public class FastSpringOrderCompletedTest {

	private static final int USER_ID = 337362;

	@Mock
	CloudUsersRepository usersRepository;
	@Mock
	DeviceInAppPurchasesRepository inApps;
	@Mock
	DeviceSubscriptionsRepository subs;
	@Mock
	UserSubscriptionService userSubService;
	@Mock
	PurchasesDataLoader purchasesDataLoader;
	@InjectMocks
	FastSpringController controller;

	private final CloudUser user = new CloudUser();

	@Before
	public void setUp() {
		user.id = USER_ID;
		user.email = "user@example.com";
		lenient().when(usersRepository.findByEmailIgnoreCase(user.email)).thenReturn(user);
	}

	@Test
	public void subscriptionIsRecordedValidForItsPeriod() throws IOException {
		String sku = "net.osmand.fastspring.subscription.pro.annual";
		when(purchasesDataLoader.getSubscriptions()).thenReturn(Map.of(sku, new PurchasesDataLoader.Subscription(
				"OsmAnd Pro", null, BooleanNode.TRUE, null, null, null, null, null, true, 1, "year", 0, 0, "fastspring")));
		FastSpringWebhookRequest request = json("order-completed-subscription.json", FastSpringWebhookRequest.class);
		assertEquals(200, controller.handleOrderCompletedEvent(request).getStatusCode().value());

		ArgumentCaptor<SupporterDeviceSubscription> saved = ArgumentCaptor.forClass(SupporterDeviceSubscription.class);
		verify(subs).saveAndFlush(saved.capture());
		SupporterDeviceSubscription s = saved.getValue();
		assertEquals("ORDER_ID_SUB_TEST00000", s.orderId);
		assertEquals("OSMANDBV000000-0000-00002", s.purchaseToken);
		assertEquals(sku, s.sku);
		assertEquals(USER_ID, (int) s.userId);
		assertTrue(s.valid);
		assertTrue(s.autorenewing);
		assertNull(s.checktime);
		Calendar expected = Calendar.getInstance();
		expected.setTime(s.starttime);
		expected.add(Calendar.YEAR, 1);
		assertEquals(expected.getTime(), s.expiretime);
		verify(userSubService).verifyAndRefreshProOrderId(user);
	}

	@Test
	public void inAppIsRecordedValid() throws IOException {
		FastSpringWebhookRequest request = json("order-completed-inapp.json", FastSpringWebhookRequest.class);
		assertEquals(200, controller.handleOrderCompletedEvent(request).getStatusCode().value());

		ArgumentCaptor<SupporterDeviceInAppPurchase> saved = ArgumentCaptor.forClass(SupporterDeviceInAppPurchase.class);
		verify(inApps).saveAndFlush(saved.capture());
		SupporterDeviceInAppPurchase p = saved.getValue();
		assertEquals("ORDER_ID_IAP_TEST00000", p.orderId);
		assertEquals("OSMANDBV000000-0000-00003", p.purchaseToken);
		assertEquals("net.osmand.fastspring.inapp.maps.plus", p.sku);
		assertEquals(USER_ID, (int) p.userId);
		assertTrue(p.valid);
		assertEquals(1789097070231L, p.purchaseTime.getTime());
		verify(userSubService).verifyAndRefreshProOrderId(user);
	}

	@Test
	public void duplicateOrderIsIgnored() throws IOException {
		FastSpringWebhookRequest request = json("order-completed-inapp.json", FastSpringWebhookRequest.class);
		when(inApps.findByOrderId("ORDER_ID_IAP_TEST00000")).thenReturn(List.of(new SupporterDeviceInAppPurchase()));
		assertEquals(200, controller.handleOrderCompletedEvent(request).getStatusCode().value());
		verify(inApps, never()).saveAndFlush(any());
	}

	// checkout requires a logged in account, so an unknown email is a deleted account or a forged hook: nothing to retry
	@Test
	public void unknownUserIsAcknowledgedWithoutRecord() throws IOException {
		FastSpringWebhookRequest request = json("order-completed-inapp.json", FastSpringWebhookRequest.class);
		request.events.get(0).data.tags.userEmail = "nobody@example.com";
		assertEquals(200, controller.handleOrderCompletedEvent(request).getStatusCode().value());
		verifyNoInteractions(inApps, subs);
	}

	// 202 + processed ids = partial accept, FastSpring retries the failed event
	@Test
	public void unknownSkuIsRejectedForRetry() throws IOException {
		FastSpringWebhookRequest request = json("order-completed-inapp.json", FastSpringWebhookRequest.class);
		request.events.get(0).data.items.get(0).sku = "net.osmand.fastspring.inapp.unknown";
		assertEquals(202, controller.handleOrderCompletedEvent(request).getStatusCode().value());
		verifyNoInteractions(inApps, subs);
	}
}
