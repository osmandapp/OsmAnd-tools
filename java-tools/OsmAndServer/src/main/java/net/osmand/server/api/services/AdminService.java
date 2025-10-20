package net.osmand.server.api.services;


import com.google.gson.Gson;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.util.Algorithms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;

@Service
public class AdminService {

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

    private final Gson gson = new Gson();

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
