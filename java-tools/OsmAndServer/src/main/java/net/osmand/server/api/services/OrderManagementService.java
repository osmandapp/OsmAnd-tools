package net.osmand.server.api.services;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.transaction.Transactional;
import net.osmand.purchases.PurchaseHelper;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.OrderInfoRepository;
import net.osmand.server.api.repo.CloudUsersRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class OrderManagementService {

	@Autowired
	private CloudUsersRepository usersRepository;

	@Autowired
	private OrderInfoRepository orderInfoRepository;

	@Autowired
	private DeviceSubscriptionsRepository subscriptionsRepository;

	@Autowired
	private DeviceInAppPurchasesRepository deviceInAppPurchasesRepository;

	@Autowired
	private HttpServletRequest request;

	@Autowired
	private UserSubscriptionService userSubService;

	@Autowired
	JdbcTemplate jdbcTemplate;

	static final String MANUALLY_VALIDATED = "manually-validated";

	private final Gson gson = new Gson();

	public List<AdminService.Purchase> searchPurchases(String q, int limit) {
		q = q
				.replace("\\", "\\\\")
				.replace("_",  "\\_")
				.replace("%",  "\\%");
		String email = q + "%";
		String order = q + "%";
		String sku = "%" + q + "%";
		String purchaseToken = q;

		String sql =
				"SELECT u.email, s.sku, s.orderid, s.purchasetoken, " +
						"       s.userid, s.timestamp, " +
						"       s.starttime, s.expiretime, s.checktime, " +
						"       s.autorenewing, s.paymentstate, s.valid, " +
						"       (s.orderid = u.orderid) AS osmand_cloud, " +
						"       NULL AS platform, NULL AS purchase_time, " +
						"       COALESCE(s.starttime, s.checktime) AS sort_key " +
						"  FROM supporters_device_sub s " +
						"  LEFT JOIN user_accounts u ON u.id = s.userid " +
						" WHERE s.sku ILIKE ? " +
						"    OR u.email ILIKE ? " +
						"    OR s.orderid ILIKE ? ESCAPE '\\' " +
						"    OR s.purchasetoken = ? " +
						"UNION ALL " +
						"SELECT u.email, i.sku, i.orderid, i.purchasetoken, " +
						"       i.userid, i.timestamp, " +
						"       NULL AS starttime, NULL AS expiretime, i.checktime, " +
						"       NULL AS autorenewing, NULL AS paymentstate, i.valid, " +
						"       FALSE AS osmand_cloud, " +
						"       i.platform, i.purchase_time, " +
						"       COALESCE(i.purchase_time, i.checktime) AS sort_key " +
						"  FROM supporters_device_iap i " +
						"  LEFT JOIN user_accounts u ON u.id = i.userid " +
						" WHERE i.sku ILIKE ? " +
						"    OR u.email ILIKE ? " +
						"    OR i.orderid ILIKE ? ESCAPE '\\' " +
						"    OR i.purchasetoken = ? " +
						"ORDER BY sort_key DESC " +
						"LIMIT ?";


		Object[] params = new Object[]{
				sku,    // first s.sku
				email, // first u.email
				order, // first s.orderid
				purchaseToken,
				sku,    // i.sku
				email, // second u.email
				order, // i.orderid
				purchaseToken,
				limit
		};

		List<AdminService.Purchase> result = jdbcTemplate.query(sql, params, (rs, rowNum) -> {
			AdminService.Purchase p = new AdminService.Purchase();
			p.email = rs.getString("email");
			p.sku = rs.getString("sku");
			p.orderId = rs.getString("orderid");
			p.purchaseToken = rs.getString("purchasetoken");
			p.userId = rs.getObject("userid") != null ? rs.getInt("userid") : null;
			p.starttime = rs.getTimestamp("starttime");
			p.expiretime = rs.getTimestamp("expiretime");
			p.checktime = rs.getTimestamp("checktime");
			p.autorenewing = rs.getObject("autorenewing") != null ? rs.getBoolean("autorenewing") : null;
			p.paymentstate = rs.getObject("paymentstate") != null ? rs.getInt("paymentstate") : null;
			p.valid = rs.getObject("valid") != null ? rs.getBoolean("valid") : null;
			p.platform = PurchaseHelper.getPlatformBySku(p.sku);
			p.purchaseTime = rs.getTimestamp("purchase_time");
			p.osmandCloud = rs.getBoolean("osmand_cloud");
			return p;
		});

		if (result.isEmpty()) {
			List<CloudUsersRepository.CloudUser> users = usersRepository.findByEmailStartingWith(q, PageRequest.of(0, limit));
			if (users != null) {
				users.forEach(u -> {
					if (u.orderid != null) {
						List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> sList = subscriptionsRepository.findByOrderId(u.orderid);
						if (sList != null && !sList.isEmpty()) {
							sList.forEach(s -> {
								AdminService.Purchase p = new AdminService.Purchase();
								p.email = u.email;
								p.sku = s.sku;
								p.orderId = s.orderId;
								p.purchaseToken = s.purchaseToken;
								p.userId = u.id;
								p.starttime = s.starttime;
								p.expiretime = s.expiretime;
								p.checktime = s.checktime;
								p.autorenewing = s.autorenewing;
								p.paymentstate = s.paymentstate;
								p.valid = s.valid;
								p.platform = null;
								p.purchaseTime = null;
								p.osmandCloud = true;
								result.add(p);
							});
						} else {
							AdminService.Purchase p = new AdminService.Purchase();
							p.email = u.email;
							p.sku = null;
							p.orderId = u.orderid;
							p.purchaseToken = null;
							p.userId = u.id;
							p.starttime = null;
							p.expiretime = null;
							p.checktime = null;
							p.autorenewing = null;
							p.paymentstate = null;
							p.valid = null;
							p.platform = null;
							p.purchaseTime = null;
							p.osmandCloud = true;
							result.add(p);
						}
					}
				});
			}

		}
		result.sort(Comparator.comparing(
				(AdminService.Purchase p) ->
						p.starttime != null
								? p.starttime
								: (p.purchaseTime != null
								? p.purchaseTime
								: p.checktime),
				Comparator.nullsLast(Comparator.naturalOrder())
		).reversed());
		return result;
	}

	public List<String> getSkus(boolean isSub, boolean isInApp) {
		if (isSub && !isInApp) {
			String sql = "SELECT DISTINCT sku FROM supporters_device_sub ORDER BY sku ASC";
			return jdbcTemplate.queryForList(sql, String.class);
		} else if (!isSub && isInApp) {
			String sql = "SELECT DISTINCT sku FROM supporters_device_iap ORDER BY sku ASC";
			return jdbcTemplate.queryForList(sql, String.class);
		}
		return Collections.emptyList();
	}

	public boolean orderWithSkuExists(String sku, String orderId) {
		List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subs =
				subscriptionsRepository.findByOrderId(orderId);
		if (subs != null) {
			for (var s : subs) {
				if (sku.equals(s.sku)) {
					return true;
				}
			}
		}
		List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> iaps =
				deviceInAppPurchasesRepository.findByOrderId(orderId);
		if (iaps != null) {
			for (var i : iaps) {
				if (sku.equals(i.sku)) {
					return true;
				}
			}
		}
		return false;
	}

	@Transactional
	public void registerNewOrder(String email,
	                             String sku,
	                             Integer period,
	                             String interval,
	                             String orderId,
	                             String purchaseToken) {
		CloudUsersRepository.CloudUser pu = usersRepository.findByEmailIgnoreCase(email);
		if (pu == null) {
			throw new IllegalArgumentException("User with email “" + email + "” not found");
		}
		int userId = pu.id;

		if (period != null && period != 0 && interval != null) {
			DeviceSubscriptionsRepository.SupporterDeviceSubscription s =
					new DeviceSubscriptionsRepository.SupporterDeviceSubscription();
			s.userId = userId;
			s.sku = sku;
			s.orderId = orderId;
			s.purchaseToken = purchaseToken;
			s.starttime = new Date();

			Calendar cal = Calendar.getInstance();
			cal.setTime(s.starttime);
			switch (interval.toLowerCase(Locale.ROOT)) {
				case "days":
					cal.add(Calendar.DAY_OF_MONTH, period);
					break;
				case "months":
					cal.add(Calendar.MONTH, period);
					break;
				case "years":
					cal.add(Calendar.YEAR, period);
					break;
				default:
					throw new IllegalArgumentException("Unknown interval: " + interval);
			}
			s.expiretime = cal.getTime();
			s.valid = purchaseToken.equals(MANUALLY_VALIDATED);
			s.checktime = purchaseToken.equals(MANUALLY_VALIDATED) ? new Date() : null;

			subscriptionsRepository.saveAndFlush(s);

			
		} else {
			DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase i =
					new DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase();
			i.userId = userId;
			i.sku = sku;
			i.orderId = orderId;
			i.purchaseToken = purchaseToken;
			i.purchaseTime = new Date();
			i.timestamp = new Date();
			i.valid = purchaseToken.equals(MANUALLY_VALIDATED);
			i.checktime = purchaseToken.equals(MANUALLY_VALIDATED) ? new Date() : null;

			deviceInAppPurchasesRepository.saveAndFlush(i);
		}
		userSubService.verifyAndRefreshProOrderId(pu);
	}

	public List<AdminService.Purchase> findPurchaseByOrderAndSku(String orderId, String sku) {
		List<AdminService.Purchase> result = new ArrayList<>();
		List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subs =
				subscriptionsRepository.findByOrderIdAndSku(orderId, sku);
		if (subs != null) {
			subs.forEach(s -> {
				CloudUsersRepository.CloudUser user = usersRepository.findById(s.userId);
				AdminService.Purchase p = new AdminService.Purchase();
				p.email = user.email;
				p.sku = s.sku;
				p.orderId = s.orderId;
				p.purchaseToken = s.purchaseToken;
				p.userId = s.userId;
				p.timestamp = s.timestamp;
				p.starttime = s.starttime;
				p.expiretime = s.expiretime;
				p.checktime = s.checktime;
				p.autorenewing = s.autorenewing;
				p.paymentstate = s.paymentstate;
				p.valid = s.valid;
				p.platform = null;
				p.purchaseTime = null;
				p.osmandCloud = s.orderId.equals(user.orderid);
				result.add(p);
			});
		}
		List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> iaps =
				deviceInAppPurchasesRepository.findByOrderIdAndSku(orderId, sku);
		if (iaps != null) {
			iaps.forEach(i -> {
				CloudUsersRepository.CloudUser user = usersRepository.findById(i.userId);
				AdminService.Purchase p = new AdminService.Purchase();
				p.email = user.email;
				p.sku = i.sku;
				p.orderId = i.orderId;
				p.purchaseToken = i.purchaseToken;
				p.userId = i.userId;
				p.timestamp = i.timestamp;
				p.starttime = null;
				p.expiretime = null;
				p.checktime = i.checktime;
				p.autorenewing = null;
				p.paymentstate = null;
				p.valid = i.valid;
				p.platform = PurchaseHelper.getPlatformBySku(i.sku);
				p.purchaseTime = i.purchaseTime;
				p.osmandCloud = false;
				result.add(p);
			});
		}
		return result;
	}

	public List<OrderInfoRepository.OrderInfoDto> listOrderVersions(String sku, String orderId) {
		return orderInfoRepository
				.findBySkuAndOrderIdOrderByUpdateTimeDesc(sku, orderId)
				.stream()
				.map(e -> new OrderInfoRepository.OrderInfoDto(
						e.sku,
						e.orderId,
						e.updateTime,
						e.editorId,
						e.details.toString()
				)).toList();
	}


	@Transactional
	public boolean saveNewOrderVersion(String sku, String orderId, String details) {
		OrderInfoRepository.OrderInfo oi = new OrderInfoRepository.OrderInfo();
		JsonObject json = gson.fromJson(details, JsonObject.class);
		oi.sku = sku;
		oi.orderId = orderId;
		oi.updateTime = new Date();
		oi.editorId = request.getUserPrincipal().getName();
		oi.details = json;
		orderInfoRepository.saveAndFlush(oi);

		return true;
	}
}

