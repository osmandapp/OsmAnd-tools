package net.osmand.server.fs;

import static net.osmand.server.fs.FastSpringSubscriptionsGetTest.json;
import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import net.osmand.purchases.FastSpringHelper;
import net.osmand.purchases.FastSpringHelper.FastSpringOrder;
import net.osmand.purchases.FastSpringHelper.FastSpringPurchase;

// GET /orders/{id} responses: src/test/resources/fs/orders-get-*.json
public class FastSpringOrdersGetTest {

	private static final String SKU = "net.osmand.fastspring.inapp.maps.plus";

	// full refund: order stays completed=true, refund is only visible in returns[]
	@Test
	public void refundedInAppIsInvalid() throws IOException {
		FastSpringOrder order = json("orders-get-refunded.json", FastSpringOrder.class);
		FastSpringPurchase p = FastSpringHelper.inAppPurchase(order, SKU);
		assertNotNull(p);
		assertEquals(1788192806114L, (long) p.purchaseTime);
		assertFalse("refunded in-app must not be valid", p.isValid());
	}
}
