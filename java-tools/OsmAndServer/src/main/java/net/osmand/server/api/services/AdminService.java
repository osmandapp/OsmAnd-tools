package net.osmand.server.api.services;


import com.google.gson.Gson;
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
