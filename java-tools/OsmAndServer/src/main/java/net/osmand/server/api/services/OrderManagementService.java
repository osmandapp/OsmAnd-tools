package net.osmand.server.api.services;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.transaction.Transactional;
import net.osmand.purchases.FastSpringHelper;
import net.osmand.purchases.PurchaseHelper;
import net.osmand.server.PurchasesDataLoader;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.OrderInfoRepository;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.controllers.pub.UserdataController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

import static net.osmand.purchases.UpdateSubscription.MINIMUM_WAIT_TO_REVALIDATE;

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
	private UserdataService userdataService;

	@Autowired
	JdbcTemplate jdbcTemplate;

	protected static final Log LOG = LogFactory.getLog(OrderManagementService.class);

	static final String MANUALLY_VALIDATED = "manually-validated";

	private final Gson gson = new Gson();

	public List<AdminService.Purchase> searchPurchases(String q, int limit) {
		q = q
				.replace("\\", "\\\\")
				.replace("_",  "\\_")
				.replace("%",  "\\%");
		if (q.contains("..")) { // q.startsWith("GPA") && 
			// remove postfix
			q = q.substring(0, q.indexOf(".."));
		}
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
						"       NULL AS purchase_time, " +
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
						"       i.purchase_time, " +
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
			CloudUsersRepository.CloudUser pu = p.userId != null ? usersRepository.findById(p.userId) : null;
			p.cloudUserInfo = pu != null ? getCloudInfo(pu) : null;
			return p;
		});

		if (result.isEmpty()) {
			List<CloudUsersRepository.CloudUser> users = usersRepository.findByEmailStartingWith(q, PageRequest.of(0, limit));
			if (users != null) {
				users.forEach(u -> {
					AdminService.CloudUserInfo cloudInfo = getCloudInfo(u);
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
								p.cloudUserInfo = cloudInfo;
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
							p.cloudUserInfo = cloudInfo;
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

	public AdminService.CloudUserInfo getCloudInfo(CloudUsersRepository.CloudUser user) {
		AdminService.CloudUserInfo info = new AdminService.CloudUserInfo();
		info.nickname = user.nickname;
		info.tokenTime = user.tokenTime;
		info.regTime = user.regTime;
		UserdataController.UserFilesResults res = userdataService.generateFiles(user.id, null, false, false, Collections.emptySet());
		info.filesCount = res.totalFiles;
		return info;
	}

	public List<String> getSkus(boolean isSub, boolean isInApp, String platform, PurchasesDataLoader purchasesDataLoader) {
		if ("all".equalsIgnoreCase(platform)) {
			// return all skus from DB
			return getSkus(isSub, isInApp);
		}

		String wanted = platform.trim().toLowerCase();
		if (wanted.isEmpty()) {
			return Collections.emptyList();
		}

		if (isSub && !isInApp) {
			Map<String, PurchasesDataLoader.Subscription> subMap = purchasesDataLoader.getSubscriptions();
			return subMap.keySet().stream()
					.map(OrderManagementService::safeSku)
					.filter(Objects::nonNull)
					.filter(sku -> normalizePlatform(PurchaseHelper.getPlatformBySku(sku)).equals(wanted))
					.distinct()
					.sorted()
					.toList();
		} else if (!isSub && isInApp) {
			Map<String, PurchasesDataLoader.InApp> inappMap = purchasesDataLoader.getInApps();
			return inappMap.keySet().stream()
					.map(OrderManagementService::safeSku)
					.filter(Objects::nonNull)
					.filter(sku -> normalizePlatform(PurchaseHelper.getPlatformBySku(sku)).equals(wanted))
					.distinct()
					.sorted()
					.toList();
		}
		return Collections.emptyList();
	}

	private static String safeSku(String sku) {
		return (sku == null || sku.isBlank()) ? null : sku.trim();
	}

	private static String normalizePlatform(String raw) {
		if (raw == null) return "";
		String p = raw.trim().toLowerCase();
		if ("fastspring".equals(p)) return "web";
		if ("google".equals(p)) return "android";
		if ("apple".equals(p)) return "ios";
		return p;
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
	public void registerNewOrder(String email, String sku, Integer period, String interval, String orderId,
			String purchaseToken, boolean isSubscription) {
		if (isSubscription) {
			if (period == null || period <= 0 || interval == null) {
				throw new IllegalArgumentException("For subscriptions, period and interval are required");
			}
		}
		CloudUsersRepository.CloudUser pu = usersRepository.findByEmailIgnoreCase(email);
		if (pu == null) {
			throw new IllegalArgumentException("User with email “" + email + "” not found");
		}
		int userId = pu.id;

		if (isSubscription) {
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
			LOG.info("Registering (/orders/register) new subscription for user " + email + ", sku: " + sku + ", orderId: " + orderId);
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
			LOG.info("Registering (/orders/register) new in-app purchase for user " + email + ", sku: " + sku + ", orderId: " + orderId);
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

	@Transactional
	public void revalidateFastspringPurchase(AdminService.Purchase purchase) {
		if (purchase == null || purchase.orderId == null || purchase.sku == null) {
			return;
		}

		boolean isSubscriptionSku = FastSpringHelper.subscriptionSkuMap.contains(purchase.sku);
		if (isSubscriptionSku) {
			resetFastSpringSubscriptionRecord(purchase);
			return;
		}

		boolean isInAppSku = FastSpringHelper.productSkuMap.contains(purchase.sku);
		if (isInAppSku) {
			resetFastSpringInAppRecord(purchase);
		}
	}

	public void resetFastSpringSubscriptionRecord(AdminService.Purchase purchase) {
		List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subs =
				subscriptionsRepository.findByOrderIdAndSku(purchase.orderId, purchase.sku);
		if (subs == null || subs.isEmpty()) {
			return;
		}
		for (DeviceSubscriptionsRepository.SupporterDeviceSubscription sub : subs) {
			sub.valid = null;
			if (shouldReset(sub.checktime)) {
				sub.checktime = null;
			}
		}
		subscriptionsRepository.saveAll(subs);
		subscriptionsRepository.flush();
	}

	private void resetFastSpringInAppRecord(AdminService.Purchase purchase) {
		List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> inApps =
				deviceInAppPurchasesRepository.findByOrderIdAndSku(purchase.orderId, purchase.sku);
		if (inApps == null || inApps.isEmpty()) {
			return;
		}

		for (DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase inApp : inApps) {
			inApp.valid = null;
			if (shouldReset(inApp.checktime)) {
				inApp.checktime = null;
			}
		}
		deviceInAppPurchasesRepository.saveAll(inApps);
		deviceInAppPurchasesRepository.flush();
	}

	private boolean shouldReset(Date checkTime) {
		if (checkTime == null) {
			return false;
		}
		return (System.currentTimeMillis() - checkTime.getTime()) <= MINIMUM_WAIT_TO_REVALIDATE;
	}
}

