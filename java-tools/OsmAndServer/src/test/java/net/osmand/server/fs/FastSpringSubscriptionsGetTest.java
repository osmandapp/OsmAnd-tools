package net.osmand.server.fs;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Objects;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.gson.Gson;

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

	private static FastSpringSubscription response(String name) throws IOException {
		try (InputStream in = FastSpringSubscriptionsGetTest.class.getResourceAsStream("/fs/" + name)) {
			return new Gson().fromJson(new InputStreamReader(Objects.requireNonNull(in, name)), FastSpringSubscription.class);
		}
	}

	// record as written by the order.completed hook an hour ago
	private static SupporterDeviceSubscription hookRecord() {
		SupporterDeviceSubscription s = new SupporterDeviceSubscription();
		s.sku = SKU;
		s.orderId = ORDER;
		s.timestamp = new Date(System.currentTimeMillis() - HOUR);
		s.starttime = s.timestamp;
		s.expiretime = new Date(s.timestamp.getTime() + 30 * 24 * HOUR);
		s.valid = true;
		s.autorenewing = true;
		return s;
	}

	@Test
	public void canceledStaysValidUntilDeactivationDate() throws IOException {
		SupporterDeviceSubscription s = hookRecord();
		try (MockedStatic<FastSpringHelper> fs = mockStatic(FastSpringHelper.class)) {
			fs.when(() -> FastSpringHelper.getSubscriptionByOrderIdAndSku(ORDER, SKU))
					.thenReturn(response("subscriptions-get-canceled.json"));
			service.revalidateFastSpringSubscription(s);
		}
		assertTrue("canceled subscription is active on FastSpring until deactivationDate, valid must stay true", s.valid);
		assertFalse("canceled subscription must not autorenew", s.autorenewing);
		assertEquals("expiretime must be FastSpring deactivationDate (2026-10-07)", 1791331200000L, s.expiretime.getTime());
		verify(repo).save(s);
	}
}
