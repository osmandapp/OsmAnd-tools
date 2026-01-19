package net.osmand.server.ws;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import io.github.bucket4j.Bucket;


@Service
public class RateLimitService {

	private static final Log LOG = LogFactory.getLog(RateLimitService.class);

	public static final String UNKNOWN_IP = "unknown";
	private static final int MAX_FAILED_ATTEMPTS = 10;
	private static final Duration RATE_LIMIT_WINDOW = Duration.ofMinutes(1);
	
	private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();
	
	/**
	 * Checks if the IP is rate limited (without consuming a token).
	 * @param ip client IP address
	 * @return true if rate limited (should block), false if allowed
	 */
	public boolean isRateLimited(String ip) {
		if (ip == null || ip.isEmpty() || UNKNOWN_IP.equals(ip)) {
			return false;
		}
		
		try {
			Bucket bucket = getBucket(ip);
			// Check available tokens without consuming
			return bucket.getAvailableTokens() < 1;
		} catch (Exception e) {
			LOG.error("Error checking rate limit for IP: " + ip, e);
			return false;
		}
	}
	
	/**
	 * Tracks a failed authentication attempt (consumes a token).
	 * @param ip client IP address
	 */
	public void trackFailedAttempt(String ip) {
		if (ip == null || ip.isEmpty() || UNKNOWN_IP.equals(ip)) {
			return;
		}
		
		try {
			Bucket bucket = getBucket(ip);
			bucket.tryConsume(1);
		} catch (Exception e) {
			LOG.error("Error tracking failed attempt for IP: " + ip, e);
		}
	}
	
	private Bucket getBucket(String ip) {
		return buckets.computeIfAbsent(ip, k -> Bucket.builder()
			.addLimit(limit -> limit
				.capacity(MAX_FAILED_ATTEMPTS)
				.refillIntervally(MAX_FAILED_ATTEMPTS, RATE_LIMIT_WINDOW)
			)
			.build());
	}
	
	/**
	 * Resets rate limit for the IP (e.g., on successful authentication).
	 * @param ip client IP address
	 */
	public void resetRateLimit(String ip) {
		if (ip != null && !ip.isEmpty() && !UNKNOWN_IP.equals(ip)) {
			buckets.remove(ip);
		}
	}
}
