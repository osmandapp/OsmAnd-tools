package net.osmand.server.fs;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.api.services.androidpublisher.model.SubscriptionPurchase;

import net.osmand.purchases.FastSpringHelper;
import net.osmand.purchases.FastSpringHelper.FastSpringSubscription;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscription;
import net.osmand.server.api.services.UserSubscriptionService;

// GET /subscriptions/{id} responses: src/test/resources/fs/subscriptions-get-*.json
@RunWith(MockitoJUnitRunner.class)
public class FastSpringSubscriptionsGetTest {

	private static final String SKU = "net.osmand.fastspring.subscription.pro.monthly";
	private static final String ORDER = "MP_ORDER_ID_TEST000000";
	private static final long HOUR = 60 * 60 * 1000L;

	@Mock
	DeviceSubscriptionsRepository repo;
	@InjectMocks
	UserSubscriptionService service;

	// record as written by the order.completed hook an hour before "now"
	private static SupporterDeviceSubscription hookRecord(long now) {
		SupporterDeviceSubscription s = new SupporterDeviceSubscription();
		s.sku = SKU;
		s.orderId = ORDER;
		s.timestamp = new Date(now - HOUR);
		s.starttime = s.timestamp;
		s.expiretime = new Date(s.timestamp.getTime() + 30 * 24 * HOUR);
		s.valid = true;
		s.autorenewing = true;
		return s;
	}

	private SupporterDeviceSubscription revalidate(String response, long now) throws IOException {
		SupporterDeviceSubscription s = hookRecord(now);
		try (MockedStatic<FastSpringHelper> fs = mockStatic(FastSpringHelper.class, CALLS_REAL_METHODS)) {
			fs.when(() -> FastSpringHelper.getSubscriptionByOrderIdAndSku(ORDER, SKU))
					.thenReturn(FsJson.read(response, FastSpringSubscription.class));
			service.revalidateFastSpringSubscription(s, now);
		}
		return s;
	}

	@Test
	public void activeTakesExpireTimeFromNextBillingDate() throws IOException {
		SupporterDeviceSubscription s = revalidate("subscriptions-get-active.json", 1789120579204L); // 2026-09-11
		assertTrue(s.valid);
		assertTrue(s.autorenewing);
		assertEquals("expiretime must be FastSpring next billing date (2027-09-11)", 1820620800000L, s.expiretime.getTime());
		verify(repo).save(s);
	}

	@Test
	public void canceledStaysValidUntilDeactivationDate() throws IOException {
		SupporterDeviceSubscription s = revalidate("subscriptions-get-canceled.json", 1788811865783L); // 2026-09-07
		assertTrue("canceled subscription is active on FastSpring until deactivationDate, valid must stay true", s.valid);
		assertFalse("canceled subscription must not autorenew", s.autorenewing);
		assertEquals("expiretime must be FastSpring deactivationDate (2026-10-07)", 1791331200000L, s.expiretime.getTime());
		verify(repo).save(s);
	}

	// UpdateSubscription job: what is written to supporters_device_sub
	@Test
	public void jobWritesActiveSubscriptionFromApi() throws IOException {
		SubscriptionPurchase p = FsJson.read("subscriptions-get-active.json", FastSpringSubscription.class).toSubscriptionPurchase();
		assertEquals("SUBSCRIPTION_ID_TEST01", p.getOrderId());
		assertEquals(1789120577858L, (long) p.getStartTimeMillis());
		assertEquals(1820620800000L, (long) p.getExpiryTimeMillis());
		assertTrue(p.getAutoRenewing());
		assertEquals(39990000L, (long) p.getPriceAmountMicros());
		assertEquals("EUR", p.getPriceCurrencyCode());
	}

	@Test
	public void jobWritesCanceledSubscriptionUntilDeactivationDate() throws IOException {
		SubscriptionPurchase p = FsJson.read("subscriptions-get-canceled.json", FastSpringSubscription.class).toSubscriptionPurchase();
		assertEquals(1791331200000L, (long) p.getExpiryTimeMillis());
		assertFalse(p.getAutoRenewing());
	}

	// refund with "Cancel Related Subscriptions": active=false, next still set
	@Test
	public void deactivatedIsInvalid() throws IOException {
		SupporterDeviceSubscription s = revalidate("subscriptions-get-deactivated.json", 1788969830287L); // 2026-09-09
		assertFalse("deactivated subscription must be invalid", s.valid);
		assertFalse(s.autorenewing);
		verify(repo).save(s);
	}
}
