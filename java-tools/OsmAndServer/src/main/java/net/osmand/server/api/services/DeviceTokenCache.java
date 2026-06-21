package net.osmand.server.api.services;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.util.Algorithms;

@Component
public class DeviceTokenCache {

	private static final long CACHE_TTL = 15 * 60 * 1000; // 15 minutes in milliseconds

	private final Map<Integer, CachedInfoDevice> cacheByDeviceId = new ConcurrentHashMap<>();

	@Autowired
	protected CloudUserDevicesRepository devicesRepository;

	public static class CachedInfoDevice {
		public final CloudUserDevice device;
		public final long timestamp;
		public CloudUser user;

		CachedInfoDevice(CloudUserDevice device, CloudUser user) {
			this.device = device;
			this.user = user;
			this.timestamp = System.currentTimeMillis();
		}
	}

	// Returns a cached or freshly loaded device matching the access token, or null if the token
	// is invalid. A stale entry is dropped when the token no longer matches the database.
	public CachedInfoDevice getValidatedDevice(int deviceId, String accessToken) {
		CachedInfoDevice cached = cacheByDeviceId.get(deviceId);
		if (cached != null && (System.currentTimeMillis() - cached.timestamp) < CACHE_TTL) {
			if (Algorithms.stringsEqual(cached.device.accesstoken, accessToken)) {
				return cached;
			}
		}
		CloudUserDevice d = devicesRepository.findById(deviceId);
		if (d != null && Algorithms.stringsEqual(d.accesstoken, accessToken)) {
			CachedInfoDevice cache = new CachedInfoDevice(d, null);
			cacheByDeviceId.put(deviceId, cache);
			return cache;
		}
		cacheByDeviceId.remove(deviceId);
		return null;
	}

	// Drops the cached entry for a device (call after deleting the device or rotating its token).
	public void invalidate(int deviceId) {
		cacheByDeviceId.remove(deviceId);
	}

	// Drops all cached entries belonging to a user (call after deleting an account).
	public void invalidateByUserId(int userId) {
		cacheByDeviceId.values().removeIf(c -> c.device.userid == userId);
	}

	// Removes expired entries from memory. Runs at the TTL interval (15 min).
	@Scheduled(fixedRate = 15, timeUnit = TimeUnit.MINUTES)
	public void clearExpired() {
		long now = System.currentTimeMillis();
		cacheByDeviceId.values().removeIf(c -> (now - c.timestamp) > CACHE_TTL);
	}
}
