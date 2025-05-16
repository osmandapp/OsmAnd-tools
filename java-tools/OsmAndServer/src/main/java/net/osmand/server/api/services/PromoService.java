package net.osmand.server.api.services;

import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.PromoCampaignRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import jakarta.transaction.Transactional;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class PromoService {

	@Autowired
	PromoCampaignRepository promoCampaignRepository;

	@Autowired
	private EmailSenderService emailSender;

	@Autowired
	private CloudUsersRepository usersRepository;

	@Autowired
	protected UserSubscriptionService userSubService;

	@Autowired
	private DeviceSubscriptionsRepository subscriptionsRepository;

	public void register(PromoCampaignRepository.Promo promo) {
		if (promoCampaignRepository.existsById(promo.name)) {
			throw new IllegalStateException("Promo already exists");
		} else {
			promoCampaignRepository.save(promo);
		}
	}

	@Transactional
	public ResponseEntity<String> addUser(String name, String email) {
		PromoCampaignRepository.Promo promoCampaign = promoCampaignRepository.findByName(name);
		ResponseEntity<String> error = validatePromo(promoCampaign);
		if (error == null) {
			String key = "promo_" + promoCampaign.name;
			Date expireTime = getExpirationDate(promoCampaign);
			PromoResponse resp = createPromoSubscription(email, key, expireTime);
			CloudUsersRepository.CloudUser existingUser = usersRepository.findByEmailIgnoreCase(email);
			if (existingUser != null && !resp.error) {
				promoCampaign.used++;
				promoCampaign.lastUsers = getLastUsers(promoCampaign);
				promoCampaignRepository.save(promoCampaign);
				return ResponseEntity.ok(expireTime.toString());
			} else {
				return ResponseEntity.badRequest().body(resp.deviceSub.purchaseToken);
			}
		}
		return error;
	}

	private ResponseEntity<String> validatePromo(PromoCampaignRepository.Promo promo) {
		if (promo != null) {
			long endTime = promo.endTime.getTime();
			long today = new Date().getTime();
			if (endTime > today) {
				if (promo.used < promo.numberLimit) {
					return null;
				} else {
					return ResponseEntity.badRequest().body("Unfortunately we ran out of available promocodes.");
				}
			} else {
				String startDate = getDateWithoutTime(promo.startTime);
				String endDate = getDateWithoutTime(promo.endTime);
				return ResponseEntity.badRequest().body(
						String.format("Promo campaign already over. Valid only from %s to %s.", startDate, endDate));
			}
		}
		return ResponseEntity.badRequest().body("Promo campaign hasn't been created yet.");
	}

	private String getDateWithoutTime(Date date) {
		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		return formatter.format(date);
	}

	private String getLastUsers(PromoCampaignRepository.Promo promoCampaign) {
		StringBuilder res = new StringBuilder();
		List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions = subscriptionsRepository
				.findFirst5BySkuOrderByStarttimeDesc("promo_" + promoCampaign.name);
		for (DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription : subscriptions) {
			CloudUsersRepository.CloudUser user = usersRepository.findByOrderid(subscription.orderId);
			if (user != null) {
				res.append(user.email).append(" | ");
			}
		}
		String emails = res.toString();
		return emails.substring(0, emails.length() - 3);
	}

	public PromoResponse createPromoSubscription(String email, String key, Date expireTime) {
		email = email.toLowerCase().trim();
		DeviceSubscriptionsRepository.SupporterDeviceSubscription deviceSub = new DeviceSubscriptionsRepository.SupporterDeviceSubscription();
		deviceSub.sku = key;
		deviceSub.orderId = UUID.randomUUID().toString();
		deviceSub.kind = "promo";
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(System.currentTimeMillis());
		deviceSub.timestamp = c.getTime();
		deviceSub.starttime = c.getTime();
		deviceSub.valid = true;
		deviceSub.purchaseToken = email;
		if (expireTime != null) {
			deviceSub.expiretime = expireTime;
		} else {
			c.add(Calendar.YEAR, 1);
			deviceSub.expiretime = c.getTime();
		}
		boolean error = false;
		if (emailSender.isEmail(email)) {
			CloudUsersRepository.CloudUser existingUser = usersRepository.findByEmailIgnoreCase(email);
			if (existingUser == null) {
				CloudUsersRepository.CloudUser pu = new CloudUsersRepository.CloudUser();
				pu.email = email;
				pu.regTime = new Date();
				pu.orderid = deviceSub.orderId;
				usersRepository.saveAndFlush(pu);
				deviceSub.purchaseToken += " (email sent & registered)";
				deviceSub.userId = pu.id; 
				subscriptionsRepository.save(deviceSub);
				emailSender.sendOsmAndCloudPromoEmail(email, deviceSub.orderId);
			} else {
				if (existingUser.orderid == null || userSubService.checkOrderIdPro(existingUser.orderid) != null) {
					existingUser.orderid = deviceSub.orderId;
					deviceSub.userId = existingUser.id;
					deviceSub.purchaseToken += " (new PRO subscription is updated)";
					subscriptionsRepository.save(deviceSub);
					usersRepository.saveAndFlush(existingUser);
				} else {
					error = true;
					deviceSub.purchaseToken += " (ERROR: user already has PRO subscription)";
				}
			}
		} else {
			error = true;
			deviceSub.purchaseToken += " (ERROR: please enter email only)";
		}
		// TODO this code should be used everywhere to update pro which could be inapp or sub
//		String errorMsg = userSubService.checkOrderIdPremium(pu.orderid);
//		if (errorMsg != null) {
//			userSubService.updateOrderId(pu);
//		}

		if (!error) {
			return new PromoResponse(deviceSub, false);
		}
		return new PromoResponse(deviceSub, true);
	}

	public static class PromoResponse {
		public DeviceSubscriptionsRepository.SupporterDeviceSubscription deviceSub;
		public boolean error;

		public PromoResponse(DeviceSubscriptionsRepository.SupporterDeviceSubscription deviceSub, boolean error) {
			this.deviceSub = deviceSub;
			this.error = error;
		}
	}

	private Date getExpirationDate(PromoCampaignRepository.Promo promo) {
		Calendar instance = Calendar.getInstance();
		instance.setTime(new Date());
		instance.add(Calendar.MONTH, promo.subActiveMonths);

		return instance.getTime();
	}

}
