package net.osmand.server.api.services;

import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

@Service
public class UserdataService {
    
    public PremiumUserDevicesRepository.PremiumUserDevice checkUser() {
        Object user = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        if (user instanceof WebSecurityConfiguration.OsmAndProUser) {
            return ((WebSecurityConfiguration.OsmAndProUser) user).getUserDevice();
        }
        return null;
    }
}
