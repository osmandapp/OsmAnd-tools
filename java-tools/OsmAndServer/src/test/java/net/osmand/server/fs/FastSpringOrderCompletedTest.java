package net.osmand.server.fs;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.node.BooleanNode;

import net.osmand.purchases.FastSpringHelper;
import net.osmand.purchases.FastSpringHelper.FastSpringSubscription;
import net.osmand.server.PurchasesDataLoader;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscription;
import net.osmand.server.api.services.EmailSenderService;
import net.osmand.server.api.services.UserSubscriptionService;
import net.osmand.server.controllers.pub.FastSpringController;
import net.osmand.server.controllers.pub.FastSpringController.FastSpringWebhookRequest;

// order.completed webhooks: src/test/resources/fs/order-completed-*.json
@RunWith(MockitoJUnitRunner.class)
public class FastSpringOrderCompletedTest {

	private static final int USER_ID = 337362;
	private static final String SUB_SKU = "net.osmand.fastspring.subscription.pro.annual";

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
	@Mock
	EmailSenderService emailSender;
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
	public void subscriptionIsRecordedValidForItsPeriod() throws Exception {
		SupporterDeviceSubscription s = recordSubscription(false, null);
		assertEquals("ORDER_ID_SUB_TEST00000", s.orderId);
		assertEquals("OSMANDBV000000-0000-00002", s.purchaseToken);
		assertEquals(SUB_SKU, s.sku);
		assertEquals(USER_ID, (int) s.userId);
		assertTrue(s.valid);
		assertTrue(s.autorenewing);
		assertNull(s.checktime);
		Calendar expected = Calendar.getInstance();
		expected.setTime(s.starttime);
		expected.add(Calendar.YEAR, 1);
		assertEquals(expected.getTime(), s.expiretime);
		verify(userSubService).verifyAndRefreshProOrderId(user);

		Receipt receipt = sentReceipt("ORDER_ID_SUB_TEST00000", "nl", "€ 19,99");
		assertTrue(receipt.purchases.isEmpty());
		assertEquals(1, receipt.subscriptions.size());
		assertSame(s, receipt.subscriptions.get(0));
	}

	@Test
	public void autorenewIsTakenFromApi() throws Exception {
		FastSpringSubscription manual = FsJson.read("subscriptions-get-active.json", FastSpringSubscription.class);
		manual.autoRenew = false;
		assertFalse(recordSubscription(true, () -> manual).autorenewing);
	}

	@Test
	public void canceledSubscriptionFromApiDoesNotRenew() throws Exception {
		FastSpringSubscription canceled = FsJson.read("subscriptions-get-canceled.json", FastSpringSubscription.class);
		assertFalse(recordSubscription(true, () -> canceled).autorenewing);
	}

	@Test
	public void estimateIsKeptWhenApiFails() throws Exception {
		assertTrue(recordSubscription(true, () -> {
			throw new IOException("FastSpring is down");
		}).autorenewing);
	}

	@Test
	public void estimateIsKeptWhenApiHasNoAutorenewYet() throws Exception {
		FastSpringSubscription partial = FsJson.read("subscriptions-get-active.json", FastSpringSubscription.class);
		partial.autoRenew = null;
		assertTrue(recordSubscription(true, () -> partial).autorenewing);
	}

	private SupporterDeviceSubscription recordSubscription(boolean apiConfigured, Callable<FastSpringSubscription> api)
			throws Exception {
		when(purchasesDataLoader.getSubscriptions()).thenReturn(Map.of(SUB_SKU, new PurchasesDataLoader.Subscription(
				"OsmAnd Pro", null, BooleanNode.TRUE, null, null, null, null, null, true, 1, "year", 0, 0, "fastspring")));
		FastSpringWebhookRequest request = FsJson.read("order-completed-subscription.json", FastSpringWebhookRequest.class);
		try (MockedStatic<FastSpringHelper> fs = mockStatic(FastSpringHelper.class, CALLS_REAL_METHODS)) {
			fs.when(FastSpringHelper::isConfigured).thenReturn(apiConfigured);
			if (api != null) {
				fs.when(() -> FastSpringHelper.getSubscriptionByOrderIdAndSku("ORDER_ID_SUB_TEST00000", SUB_SKU))
						.thenAnswer(invocation -> api.call());
			}
			assertEquals(200, controller.handleOrderCompletedEvent(request).getStatusCode().value());
			if (!apiConfigured) {
				fs.verify(() -> FastSpringHelper.getSubscriptionByOrderIdAndSku(anyString(), anyString()), never());
			}
		}
		ArgumentCaptor<SupporterDeviceSubscription> saved = ArgumentCaptor.forClass(SupporterDeviceSubscription.class);
		verify(subs).saveAndFlush(saved.capture());
		return saved.getValue();
	}

	@Test
	public void inAppIsRecordedValid() throws IOException {
		FastSpringWebhookRequest request = FsJson.read("order-completed-inapp.json", FastSpringWebhookRequest.class);
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

		Receipt receipt = sentReceipt("ORDER_ID_IAP_TEST00000", "es", "$1,299.00 MXN");
		assertTrue(receipt.subscriptions.isEmpty());
		assertEquals(1, receipt.purchases.size());
		assertSame(p, receipt.purchases.get(0));
		assertEquals(p.purchaseTime, receipt.orderDate);
	}

	private record Receipt(Date orderDate, List<SupporterDeviceInAppPurchase> purchases,
	                       List<SupporterDeviceSubscription> subscriptions) {
	}

	@SuppressWarnings("unchecked")
	private Receipt sentReceipt(String orderId, String lang, String total) {
		ArgumentCaptor<Runnable> afterCommit = ArgumentCaptor.forClass(Runnable.class);
		verify(emailSender, atLeastOnce()).sendAfterCommit(afterCommit.capture());
		afterCommit.getAllValues().forEach(Runnable::run);
		ArgumentCaptor<Date> orderDate = ArgumentCaptor.forClass(Date.class);
		ArgumentCaptor<List<SupporterDeviceInAppPurchase>> purchases = ArgumentCaptor.forClass(List.class);
		ArgumentCaptor<List<SupporterDeviceSubscription>> subscriptions = ArgumentCaptor.forClass(List.class);
		verify(emailSender).sendPurchaseReceiptEmail(eq(user.email), eq(USER_ID), eq(lang), eq(orderId),
				orderDate.capture(), eq(total), purchases.capture(), subscriptions.capture());
		return new Receipt(orderDate.getValue(), purchases.getValue(), subscriptions.getValue());
	}

	@Test
	public void duplicateOrderIsIgnored() throws IOException {
		FastSpringWebhookRequest request = FsJson.read("order-completed-inapp.json", FastSpringWebhookRequest.class);
		when(inApps.findByOrderId("ORDER_ID_IAP_TEST00000")).thenReturn(List.of(new SupporterDeviceInAppPurchase()));
		assertEquals(200, controller.handleOrderCompletedEvent(request).getStatusCode().value());
		verify(inApps, never()).saveAndFlush(any());
	}

	// checkout requires a logged in account, so an unknown email is a deleted account or a forged hook: nothing to retry
	@Test
	public void unknownUserIsAcknowledgedWithoutRecord() throws IOException {
		FastSpringWebhookRequest request = FsJson.read("order-completed-inapp.json", FastSpringWebhookRequest.class);
		request.events.get(0).data.tags.userEmail = "nobody@example.com";
		assertEquals(200, controller.handleOrderCompletedEvent(request).getStatusCode().value());
		verifyNoInteractions(inApps, subs);
	}

	// 202 + processed ids = partial accept, FastSpring retries the failed event
	@Test
	public void unknownSkuIsRejectedForRetry() throws IOException {
		FastSpringWebhookRequest request = FsJson.read("order-completed-inapp.json", FastSpringWebhookRequest.class);
		request.events.get(0).data.items.get(0).sku = "net.osmand.fastspring.inapp.unknown";
		assertEquals(202, controller.handleOrderCompletedEvent(request).getStatusCode().value());
		verifyNoInteractions(inApps, subs);
	}
}
