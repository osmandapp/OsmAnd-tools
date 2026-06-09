package net.osmand.server.api.services;


import com.google.gson.Gson;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.util.Algorithms;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;

@Service
public class AdminService {

	private static final Log LOG = LogFactory.getLog(AdminService.class);

	private static final String BACKUP_USER_FILE = "/.well-known/backup-users.txt";
	private static final String BACKUP_USERS_SQL =
			"SELECT DISTINCT 'user-' || userid || '/' FROM user_files "
					+ "WHERE updatetime > NOW() AT TIME ZONE 'UTC' - INTERVAL '2 hours'";
	public static final int BACKUP_SCHEDULED_INTERVAL_1_H = 60 * 60 * 1000;

    @Autowired
    private EmailSenderService emailSender;

    @Autowired
    private CloudUsersRepository usersRepository;

    @Autowired
    private DeviceSubscriptionsRepository subscriptionsRepository;

	@Autowired
	private DeviceInAppPurchasesRepository deviceInAppPurchasesRepository;

    @Autowired
    UserdataService userdataService;

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Value("${osmand.files.location}")
	private String filesLocation;

    private final Gson gson = new Gson();

	// Hourly: writes user ids with recent file changes to osmand.backup.users-file
	// enabled via INCREMENTAL_CLOUD_USERS_FILE.
	@Scheduled(fixedRate = BACKUP_SCHEDULED_INTERVAL_1_H)
	public void backupUserListScheduledTask() {
		if (System.getenv("INCREMENTAL_CLOUD_USERS_FILE") == null) {
			return;
		}
		try {
			List<String> users = jdbcTemplate.queryForList(BACKUP_USERS_SQL, String.class);
			Path target = Path.of(filesLocation, BACKUP_USER_FILE);
			Files.createDirectories(target.getParent());
			String content = users.isEmpty() ? "" : String.join("\n", users) + "\n";
			Path tmp = Files.createTempFile(target.getParent(), target.getFileName().toString(), "");
			Files.writeString(tmp, content, StandardCharsets.UTF_8);
			Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
			LOG.info("Updated backup users : " + users.size());
		} catch (Exception e) {
			LOG.error("Failed to update backup users file" + BACKUP_USER_FILE, e);
		}
	}

	// by orderId from pro user or email
    public DeviceSubscriptionsRepository.SupporterDeviceSubscription getSubscriptionDetailsByIdentifier(String identifier) {
        DeviceSubscriptionsRepository.SupporterDeviceSubscription deviceSub = new DeviceSubscriptionsRepository.SupporterDeviceSubscription();
        deviceSub.sku = "not found";
        deviceSub.orderId = "none";
        deviceSub.valid = false;

        if (emailSender.isEmail(identifier)) {
            CloudUsersRepository.CloudUser pu = usersRepository.findByEmailIgnoreCase(identifier);
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
			CloudUsersRepository.CloudUser pu = usersRepository.findByEmailIgnoreCase(identifier);
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
		public CloudUserInfo cloudUserInfo;
	}

	public static class CloudUserInfo implements Serializable {
		@Serial
		private static final long serialVersionUID = 1L;

		public String nickname;
		public Date tokenTime;
		public Date regTime;
		public Integer filesCount;
	}

	private String createPayloadInfo(CloudUsersRepository.CloudUser pu) {
		UserdataController.UserFilesResults ufs = userdataService.generateFiles(pu.id, null, true, false, Collections.emptySet());
		ufs.allFiles.clear();
		ufs.uniqueFiles.clear();
		return pu.email + " token:" + (Algorithms.isEmpty(pu.token) ? "none" : "sent") + " at "
				+ pu.tokenTime + "\n" + gson.toJson(ufs);
	}
}
