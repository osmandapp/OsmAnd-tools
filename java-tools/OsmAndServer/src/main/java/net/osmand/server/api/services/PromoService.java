package net.osmand.server.api.services;

import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.PremiumUsersRepository;
import net.osmand.server.api.repo.PromoRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class PromoService {
    
    @Autowired
    PromoRepository promoRepository;
    
    @Autowired
    private EmailSenderService emailSender;
    
    @Autowired
    private PremiumUsersRepository usersRepository;
    
    @Autowired
    protected UserSubscriptionService userSubService;
    
    @Autowired
    private DeviceSubscriptionsRepository subscriptionsRepository;
    
    public void register(PromoRepository.Promo promo) {
        if (promoRepository.existsById(promo.name)) {
            throw new IllegalStateException("Promo already exists");
        } else {
            promoRepository.save(promo);
        }
    }
    
    public ResponseEntity<String> addUser(String name, String email) {
        PromoRepository.Promo promo = promoRepository.findByName(name);
        if (promo.used < promo.numberLimit) {
            String key = "promo_" + promo.name;
            Date expireTime = getExpirationDate(promo);
            PromoResponse resp = createPromoSubscription(email, key, expireTime);
            PremiumUsersRepository.PremiumUser existingUser = usersRepository.findByEmail(email);
            if (existingUser != null && !resp.error) {
                promo.used++;
                promoRepository.save(promo);
                return ResponseEntity.ok(expireTime.toString());
            } else {
                return ResponseEntity.badRequest().body(resp.deviceSub.purchaseToken);
            }
        }
        return ResponseEntity.badRequest().body("Unfortunately we ran out of available promocodes");
    }
    
    
    public synchronized PromoResponse createPromoSubscription(String email, String key, Date expireTime) {
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
            PremiumUsersRepository.PremiumUser existingUser = usersRepository.findByEmail(email);
            if (existingUser == null) {
                PremiumUsersRepository.PremiumUser pu = new PremiumUsersRepository.PremiumUser();
                pu.email = email;
                pu.regTime = new Date();
                pu.orderid = deviceSub.orderId;
                usersRepository.saveAndFlush(pu);
                deviceSub.purchaseToken += " (email sent & registered)";
                emailSender.sendOsmAndCloudPromoEmail(email, deviceSub.orderId);
            } else {
                if (existingUser.orderid == null || userSubService.checkOrderIdPremium(existingUser.orderid) != null) {
                    existingUser.orderid = deviceSub.orderId;
                    usersRepository.saveAndFlush(existingUser);
                    deviceSub.purchaseToken += " (new PRO subscription is updated)";
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
            subscriptionsRepository.save(deviceSub);
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
    
    private Date getExpirationDate(PromoRepository.Promo promo) {
        Calendar instance = Calendar.getInstance();
        instance.setTime(new Date());
        instance.add(Calendar.MONTH, promo.subActiveMonths);
        
        return instance.getTime();
    }
    
    
}
