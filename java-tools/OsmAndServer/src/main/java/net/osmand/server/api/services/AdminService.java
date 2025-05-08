package net.osmand.server.api.services;


import com.google.gson.Gson;
import jakarta.transaction.Transactional;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.PremiumUsersRepository;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.util.Algorithms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;

@Service
public class AdminService {

    @Autowired
    private EmailSenderService emailSender;

    @Autowired
    private PremiumUsersRepository usersRepository;

    @Autowired
    private DeviceSubscriptionsRepository subscriptionsRepository;

	@Autowired
	private DeviceInAppPurchasesRepository deviceInAppPurchasesRepository;

	@Autowired
	JdbcTemplate jdbcTemplate;

    @Autowired
    UserdataService userdataService;

	static final String SUPPORT_VALIDATED = "supportvalidated";

    private final Gson gson = new Gson();

	// by orderId from premium user or email
    public DeviceSubscriptionsRepository.SupporterDeviceSubscription getSubscriptionDetailsByIdentifier(String identifier) {
        DeviceSubscriptionsRepository.SupporterDeviceSubscription deviceSub = new DeviceSubscriptionsRepository.SupporterDeviceSubscription();
        deviceSub.sku = "not found";
        deviceSub.orderId = "none";
        deviceSub.valid = false;

        if (emailSender.isEmail(identifier)) {
            PremiumUsersRepository.PremiumUser pu = usersRepository.findByEmailIgnoreCase(identifier);
            if (pu != null) {
                String suffix = pu.orderid != null ? " (pro email)" : " (osmand start)";
                deviceSub.sku = identifier + suffix;
                List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> ls = subscriptionsRepository.findByOrderId(pu.orderid);
                if (ls != null && !ls.isEmpty()) {
                    deviceSub = ls.get(0);
                }
                if (deviceSub != null) {
                    deviceSub.payload = createPayloadInfo(pu);
                }
            }
        } else {
            List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> ls = subscriptionsRepository.findByOrderId(identifier);
            if (ls != null && !ls.isEmpty()) {
                deviceSub = ls.get(0);
            }
        }

        return deviceSub;
    }

	public List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> getSubscriptionsByIdentifier(String identifier) {
		List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> result = Collections.emptyList();

		if (emailSender.isEmail(identifier)) {
			PremiumUsersRepository.PremiumUser pu = usersRepository.findByEmailIgnoreCase(identifier);
			if (pu != null) {
				int userid = pu.id;
				Map<String, DeviceSubscriptionsRepository.SupporterDeviceSubscription> map = new LinkedHashMap<>();
				for (DeviceSubscriptionsRepository.SupporterDeviceSubscription s : subscriptionsRepository.findAllByUserId(userid)) {
					map.put(s.orderId + s.sku, s);
				}
				String orderId = pu.orderid;
				if (orderId != null) {
					for (DeviceSubscriptionsRepository.SupporterDeviceSubscription s : subscriptionsRepository.findByOrderId(orderId)) {
						map.put(s.orderId + s.sku, s);
					}
				}
				String info = createPayloadInfo(pu);
				for (DeviceSubscriptionsRepository.SupporterDeviceSubscription s : map.values()) {
					s.payload = info;
				}
				if (!map.isEmpty()) {
					result = new ArrayList<>(map.values());
				}
			}
		} else {
			result = subscriptionsRepository.findByOrderId(identifier);
		}

		return result;
	}

	public List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> getInappsDetailsByIdentifier(String identifier) {
		if (emailSender.isEmail(identifier)) {
			return Optional.ofNullable(usersRepository.findByEmailIgnoreCase(identifier))
					.map(pu -> deviceInAppPurchasesRepository.findByUserId(pu.id))
					.filter(list -> !list.isEmpty())
					.orElse(Collections.emptyList());
		}

		return Optional.ofNullable(deviceInAppPurchasesRepository.findByOrderId(identifier))
				.filter(list -> !list.isEmpty())
				.orElse(Collections.emptyList());
	}

	public List<Purchase> searchPurchases(String q, int limit) {
		String like = "%" + q + "%";
		String sql =
				"SELECT u.email, s.sku, s.orderid, s.purchasetoken, " +
						"       s.userid, s.timestamp, " +
						"       s.starttime, s.expiretime, s.checktime, " +
						"       s.autorenewing, s.paymentstate, s.valid, " +
						"       (s.orderid = u.orderid) AS osmand_cloud, " +
						"       NULL           AS platform, NULL           AS purchase_time " +
						"  FROM supporters_device_sub s " +
						"  JOIN user_accounts    u ON u.id = s.userid " +
						" WHERE s.sku ILIKE ? OR u.email ILIKE ? OR s.orderid ILIKE ? " +
						"UNION ALL " +
						"SELECT u.email, i.sku, i.orderid, i.purchasetoken, " +
						"       i.userid, i.timestamp, " +
						"       NULL           AS starttime, NULL           AS expiretime, i.checktime, " +
						"       NULL           AS autorenewing, NULL           AS paymentstate, i.valid, " +
						"       FALSE          AS osmand_cloud, " +
						"       i.platform, i.purchase_time " +
						"  FROM supporters_device_iap i " +
						"  JOIN user_accounts    u ON u.id = i.userid " +
						" WHERE i.sku ILIKE ? OR u.email ILIKE ? OR i.orderid ILIKE ? " +
						"ORDER BY timestamp DESC " +
						"LIMIT ?";

		return jdbcTemplate.query(
				sql,
				new Object[]{like, like, like, like, like, like, limit},
				(rs, rowNum) -> {
					Purchase p = new Purchase();
					p.email = rs.getString("email");
					p.sku = rs.getString("sku");
					p.orderId = rs.getString("orderid");
					p.purchaseToken = rs.getString("purchasetoken");
					p.userId = rs.getObject("userid") != null ? rs.getInt("userid") : null;
					p.timestamp = rs.getTimestamp("timestamp");
					p.starttime = rs.getTimestamp("starttime");
					p.expiretime = rs.getTimestamp("expiretime");
					p.checktime = rs.getTimestamp("checktime");
					p.autorenewing = rs.getObject("autorenewing") != null ? rs.getBoolean("autorenewing") : null;
					p.paymentstate = rs.getObject("paymentstate") != null ? rs.getInt("paymentstate") : null;
					p.valid = rs.getObject("valid") != null ? rs.getBoolean("valid") : null;
					p.platform = rs.getString("platform");
					p.purchaseTime = rs.getTimestamp("purchase_time");
					p.osmandCloud = rs.getBoolean("osmand_cloud");
					return p;
				}
		);
	}

	public List<String> getTopSkus(int limit) {
		String sql = ""
				+ "SELECT sku FROM ("
				+ "  SELECT sku, COUNT(*) AS cnt FROM supporters_device_sub GROUP BY sku "
				+ "  UNION ALL "
				+ "  SELECT sku, COUNT(*) AS cnt FROM supporters_device_iap GROUP BY sku "
				+ ") t "
				+ "GROUP BY sku "
				+ "ORDER BY SUM(cnt) DESC "
				+ "LIMIT ?";
		return jdbcTemplate.queryForList(sql, new Object[]{limit}, String.class);
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
		PremiumUsersRepository.PremiumUser pu = usersRepository.findByEmailIgnoreCase(email);
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
			s.valid = purchaseToken.equals(SUPPORT_VALIDATED);
			s.checktime = purchaseToken.equals(SUPPORT_VALIDATED) ? new Date() : null;

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
			i.valid = purchaseToken.equals(SUPPORT_VALIDATED);
			i.checktime = purchaseToken.equals(SUPPORT_VALIDATED) ? new Date() : null;

			deviceInAppPurchasesRepository.saveAndFlush(i);
		}
	}

	public List<Purchase> findPurchaseByOrderAndSku(String orderId, String sku) {
		List<Purchase> result = new ArrayList<>();
		List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subs =
				subscriptionsRepository.findByOrderIdAndSku(orderId, sku);
		if (subs != null) {
			subs.forEach(s -> {
				PremiumUsersRepository.PremiumUser user = usersRepository.findById(s.userId);
				Purchase p = new Purchase();
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
				PremiumUsersRepository.PremiumUser user = usersRepository.findById(i.userId);
				Purchase p = new Purchase();
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
				p.platform = i.platform;
				p.purchaseTime = i.purchaseTime;
				p.osmandCloud = false;
				result.add(p);
			});
		}
		return result;
	}


	public static class Purchase implements Serializable {
		@Serial
		private static final long serialVersionUID = 1L;

		public String email;
		public String sku;
		public String orderId;
		public String purchaseToken;
		public Integer userId;
		public Date timestamp;
		public Date starttime;
		public Date expiretime;
		public Date checktime;
		public Boolean autorenewing;
		public Integer paymentstate;
		public Boolean valid;
		public String platform;
		public Date purchaseTime;
		public Boolean osmandCloud;
	}

	private String createPayloadInfo(PremiumUsersRepository.PremiumUser pu) {
		UserdataController.UserFilesResults ufs = userdataService.generateFiles(pu.id, null, true, false);
		ufs.allFiles.clear();
		ufs.uniqueFiles.clear();
		return pu.email + " token:" + (Algorithms.isEmpty(pu.token) ? "none" : "sent") + " at "
				+ pu.tokenTime + "\n" + gson.toJson(ufs);
	}
}
