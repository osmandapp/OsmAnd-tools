package net.osmand.server.api.services;

import java.io.File;
import java.time.Duration;
import java.time.LocalDate;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import net.osmand.server.traffic.TrafficFeed;
import net.osmand.server.traffic.TrafficFeeds;
import net.osmand.util.Algorithms;

@Service
public class TrafficService {

	private static final Log LOGGER = LogFactory.getLog(TrafficService.class);

	private static final int TICK_MINUTES = 5;

	@Value("${osmand.traffic.location}")
	private String trafficLocation;

	@Value("${tile-server.obf.location}")
	private String obfLocation;

	private TrafficFeeds feeds;

	// a tick can take minutes (road matching), so it runs on its own thread and not on the shared scheduler
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private Future<?> running;

	// every tick each feed decides itself whether it wants a download (from how often its source publishes)
	@Scheduled(fixedDelay = TICK_MINUTES * 60 * 1000L, initialDelay = 60 * 1000L)
	public synchronized void updateTraffic() {
		if (!isEnabled() || Algorithms.isEmpty(obfLocation) || (running != null && !running.isDone())) {
			return;
		}
		running = executor.submit(() -> {
			try {
				getFeeds().tick(Duration.ofMinutes(TICK_MINUTES));
			} catch (Exception e) {
				LOGGER.error("Traffic update failed", e);
			}
		});
	}

	// traffic runs only on a server that has the traffic folder
	public boolean isEnabled() {
		return !Algorithms.isEmpty(trafficLocation) && new File(trafficLocation).isDirectory();
	}

	public File getIndexFile() {
		return isEnabled() ? new File(trafficLocation, TrafficFeeds.INDEX_FILE) : null;
	}

	public File getDayFile(String source, String day) {
		if (!isEnabled() || !day.matches("\\d{4}-\\d{2}-\\d{2}")) {
			return null;
		}
		for (TrafficFeed feed : getFeeds().getFeeds()) {
			if (feed.id().equals(source)) {
				return feed.dayFile(LocalDate.parse(day));
			}
		}
		return null;
	}

	private synchronized TrafficFeeds getFeeds() {
		if (feeds == null) {
			feeds = new TrafficFeeds(new File(trafficLocation), new File(obfLocation));
		}
		return feeds;
	}
}
