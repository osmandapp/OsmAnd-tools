package net.osmand.server.api.services;


import com.google.gson.Gson;
import lombok.Getter;
import lombok.Setter;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.PremiumUsersRepository;
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
    private PremiumUsersRepository usersRepository;

    @Autowired
    private DeviceSubscriptionsRepository subscriptionsRepository;

	@Autowired
	private DeviceInAppPurchasesRepository deviceInAppPurchasesRepository;

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

	public List<Purchase> getUserPurchases(String identifier) {
		PremiumUsersRepository.PremiumUser pu;
		if (emailSender.isEmail(identifier)) {
			pu = usersRepository.findByEmailIgnoreCase(identifier);
		} else {
			pu = usersRepository.findById(Integer.parseInt(identifier));
		}
		if (pu == null) {
			return Collections.emptyList();
		}
		List<Purchase> purchases = new ArrayList<>();
		for (DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase iap : deviceInAppPurchasesRepository.findByUserId(pu.id)) {
			Purchase purchase = new Purchase();
			purchase.sku = iap.sku;
			purchase.orderId = iap.orderId;
			purchase.purchaseTime = iap.purchaseTime;
			purchase.valid = Boolean.TRUE.equals(iap.valid);
			purchases.add(purchase);
		}
		for (DeviceSubscriptionsRepository.SupporterDeviceSubscription sub : subscriptionsRepository.findAllByUserId(pu.id)) {
			Purchase purchase = new Purchase();
			purchase.sku = sub.sku;
			purchase.orderId = sub.orderId;
			purchase.startTime = sub.starttime;
			purchase.expireTime = sub.expiretime;
			purchase.valid = Boolean.TRUE.equals(sub.valid);
			purchase.isMainSub = sub.orderId != null && sub.orderId.equals(pu.orderid);
			purchases.add(purchase);
		}
		purchases.sort(Comparator.comparing(
				(Purchase p) -> p.purchaseTime != null ? p.purchaseTime : p.startTime
		).reversed());
		return purchases;
	}

	@Getter
	@Setter
	public static class Purchase implements Serializable {
		@Serial
		private static final long serialVersionUID = 1L;

		private String sku;
		private String orderId;
		private Date startTime;
		private Date expireTime;
		private Date purchaseTime;
		private boolean valid;
		private boolean isMainSub;
	}

	private String createPayloadInfo(PremiumUsersRepository.PremiumUser pu) {
		UserdataController.UserFilesResults ufs = userdataService.generateFiles(pu.id, null, true, false);
		ufs.allFiles.clear();
		ufs.uniqueFiles.clear();
		return pu.email + " token:" + (Algorithms.isEmpty(pu.token) ? "none" : "sent") + " at "
				+ pu.tokenTime + "\n" + gson.toJson(ufs);
	}
}
