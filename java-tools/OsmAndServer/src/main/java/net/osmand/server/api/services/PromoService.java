package net.osmand.server.api.services;

import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.PromoCampaignRepository;
import net.osmand.util.Algorithms;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import jakarta.transaction.Transactional;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static net.osmand.server.controllers.pub.SubscriptionController.PLATFORM_FASTSPRING;

@Service
public class PromoService {

	@Autowired
	PromoCampaignRepository promoCampaignRepository;

	@Autowired
	private EmailSenderService emailSender;

	@Autowired
	private CloudUsersRepository usersRepository;

	@Autowired
	@Lazy
	protected UserSubscriptionService userSubService;

	@Autowired
	private DeviceSubscriptionsRepository subscriptionsRepository;

	private static final String FASTSPRING_PROMO = "promo_user";

	private static final Log LOG = LogFactory.getLog(PromoService.class);

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
			PromoResponse resp = createPromoSubscription(email, key, expireTime, false);
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

	public PromoResponse createPromoSubscription(String email, String key, Date expireTime, boolean skipExistingPro) {
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
				if (skipExistingPro || (existingUser.orderid == null || userSubService.checkOrderIdPro(existingUser.orderid) != null)) {
					existingUser.orderid = deviceSub.orderId;
					deviceSub.userId = existingUser.id;
					deviceSub.purchaseToken += " (new PRO subscription is updated)";
					subscriptionsRepository.save(deviceSub);
					usersRepository.saveAndFlush(existingUser);
					userSubService.verifyAndRefreshProOrderId(existingUser);
				} else {
					error = true;
					deviceSub.purchaseToken += " (ERROR: user already has PRO subscription)";
				}
			}
		} else {
			error = true;
			deviceSub.purchaseToken += " (ERROR: please enter email only)";
		}

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

	private ResponseEntity<String> addFastSpringPromo(String sku, Integer userId) {
		if (!Algorithms.isEmpty(sku) && sku.contains(PLATFORM_FASTSPRING)) {
			Calendar cal = Calendar.getInstance();
			cal.set(2025, Calendar.SEPTEMBER, 1, 0, 0, 0);
			Date expireTime = cal.getTime();
			Date now = new Date();
			if (now.after(expireTime)) {
				return ResponseEntity.ok("Promo FastSpring subscription is not available after " + expireTime);
			}
			CloudUsersRepository.CloudUser pu = usersRepository.findById(userId);
			if (pu == null) {
				return userSubService.error("User not found. FastSpring promo subscription requires a registered user.");
			}
			List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions = subscriptionsRepository.findAllByUserId(userId);
			if (subscriptions != null && !subscriptions.isEmpty()) {
				for (DeviceSubscriptionsRepository.SupporterDeviceSubscription sub : subscriptions) {
					if (sub.sku != null && sub.sku.contains(FASTSPRING_PROMO)) {
						return userSubService.error("FastSpring promo subscription already exists for userId: " + userId);
					}
				}
			}
			PromoService.PromoResponse resp = createPromoSubscription(pu.email, FASTSPRING_PROMO, expireTime, true);
			if (resp != null && resp.error) {
				return userSubService.error("Failed to create FastSpring promo subscription, userId = " + userId);
			}
			return ResponseEntity.ok("Promo FastSpring subscription created successfully for userId: " + userId);
		}
		return null;
	}

	public void processFastSpringPromo(String sku, Integer userId) {
		ResponseEntity<String> promoResp = addFastSpringPromo(sku, userId);
		if (promoResp != null) {
			if (promoResp.getStatusCode() == HttpStatus.OK) {
				LOG.info(promoResp.getBody());
			} else {
				LOG.error(promoResp.getBody());
			}
		}
	}

}
