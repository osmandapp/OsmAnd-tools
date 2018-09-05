package net.osmand.server.api.services;

import net.osmand.server.api.repo.MapUserRepository;
import net.osmand.server.api.repo.SupporterSubscriptionRepository;
import net.osmand.server.api.repo.SupportersRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Optional;

@Service
public class SubscriptionService {

    private final SupportersRepository supportersRepository;
    private final SupporterSubscriptionRepository supporterSubscriptionRepository;
    private final MapUserRepository mapUserRepository;

    @Autowired
    public SubscriptionService(SupportersRepository supportersRepository,
                               SupporterSubscriptionRepository supporterSubscriptionRepository,
                               MapUserRepository mapUserRepository) {
        this.supportersRepository = supportersRepository;
        this.supporterSubscriptionRepository = supporterSubscriptionRepository;
        this.mapUserRepository = mapUserRepository;
    }

    private SupporterSubscriptionRepository.SupporterSubscription newSupporterSubscription(String userId, String sku, String purchaseToken) {
        SupporterSubscriptionRepository.SupporterSubscription supporterSubscription = new SupporterSubscriptionRepository.SupporterSubscription();
        supporterSubscription.setUserId(userId);
        supporterSubscription.setSku(sku);
        supporterSubscription.setPurchaseToken(purchaseToken);
        supporterSubscription.setTime(new Date());
        return supporterSubscription;
    }

    public ResponseEntity<String> purchase(String userId, String purchaseToken, String sku) {
        Optional<SupportersRepository.Supporter> optionalSupporter = supportersRepository.findById(userId);
        if (!optionalSupporter.isPresent()) {
            return ResponseEntity.badRequest().body("{\"error\": \"User is not found\"}");
        }
        supporterSubscriptionRepository.save(newSupporterSubscription(userId, sku, purchaseToken));
        SupportersRepository.Supporter supporter = optionalSupporter.get();
        String body = String.format("{\"status\": \"OK\", \"visibleName\": \"%s\", \"email\": \"%s\", \"preferredCountry\": \"%s\"}",
                supporter.getVisbleName(), supporter.getUserEmail(), supporter.getPreferedRegion());
        return ResponseEntity.ok().body(body);
    }

    public ResponseEntity<String> registerEmail(String aid, String email) {
        long timestamp = System.currentTimeMillis();
        MapUserRepository.MapUser mapUser = new MapUserRepository.MapUser();
        mapUser.setAid(aid);
        mapUser.setEmail(email);
        mapUser.setUpdateTime(timestamp);
        mapUserRepository.save(mapUser);
        String body = String.format("{\"status\": \"OK\", \"email\": \"%s\", \"time\": \"%d\"}", email, timestamp);
        return ResponseEntity.ok(body);
    }
}