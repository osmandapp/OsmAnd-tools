package net.osmand.server.api.services;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.server.ResponseStatusException;

import com.google.gson.Gson;

import net.osmand.server.DatasourceConfiguration;
import net.osmand.server.api.services.DownloadIndexesService.DownloadServerLoadBalancer;
import net.osmand.server.api.services.DownloadIndexesService.DownloadServerType;
import net.osmand.server.api.services.DownloadIndexesService.PredefinedServerSpecialty;
import net.osmand.server.controllers.pub.DownloadIndexController;
import net.osmand.server.monitor.OsmAndServerMonitorTasks;

@Ignore
public class DownloadIndexesServiceTest {

	private static final String IPV4 = "203.0.113.10";
	private static final String IPV6 = "2001:db8::10";
	private static final String US1 = "us1.osmand.net";
	private static final String FREE = "OsmAnd~";
	private static final String PAID = "OsmAnd+";

	private final IpLocationService locationService = mock(IpLocationService.class);
	private DownloadServerLoadBalancer settings;
	private DownloadMonitor monitor;

	@Before
	public void setUp() throws IOException {
		// Snapshot of api/download_settings.json: the same filters, weights and CIDR region.
		try (InputStream in = getClass().getResourceAsStream("/api/test_download_settings.json");
				InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
			settings = new Gson().fromJson(reader, DownloadServerLoadBalancer.class);
		}
		settings.prepare();
	}

	@Test
	public void northAmericanClientsUseUs1ForAllFiveTypesOverIpv4AndIpv6() {
		for (String ip : new String[] { IPV4, IPV6 }) {
			for (String country : new String[] { "CA", "MX", "US" }) {
				country(ip, country);
				assertPool(PredefinedServerSpecialty.MAIN, "usMain", ip);
				assertPool(PredefinedServerSpecialty.ROADS, "usRoads", ip);
				assertPool(PredefinedServerSpecialty.WIKI, "usWiki", ip);
				assertPool(PredefinedServerSpecialty.SRTM, "usSrtm", ip);
				assertPool(PredefinedServerSpecialty.HEIGHTMAP, "usHeightmap", ip);
			}
		}
	}

	@Test
	public void otherAndUnknownCountriesUseStandardPools() {
		for (String ip : new String[] { IPV4, IPV6 }) {
			for (String country : new String[] { "DE", "CU", "" }) {
				country(ip, country);
				assertStandardPools(ip);
			}
		}
	}

	@Test
	public void freeMainWinsBeforeGeoIpIsNeeded() {
		assertEquals("freeMain", pool(PredefinedServerSpecialty.MAIN, FREE, IPV4).key);
		verifyNoInteractions(locationService);
	}

	@Test
	public void priorityFollowsServersOrder() {
		country(IPV4, "US");
		assertEquals("freeMain", pool(PredefinedServerSpecialty.MAIN, FREE, IPV4).key);

		// Moving freeMain after usMain in servers changes which matching filter wins.
		Map<String, Integer> freeMain = settings.servers.remove("freeMain");
		settings.servers.put("freeMain", freeMain);
		assertEquals("usMain", pool(PredefinedServerSpecialty.MAIN, FREE, IPV4).key);
	}

	@Test
	public void internalRequestsWithoutGeoIpUseStandardPool() {
		assertEquals("main", settings.getServerType(PredefinedServerSpecialty.MAIN, "").key);
		verifyNoInteractions(locationService);
	}

	@Test
	public void everyMonitoredHostEntersDowntimeAndIsRemovedOnRecovery() {
		Map<?, ?> downtime = useDowntime();
		assertTrue(downtime.isEmpty());
		assertTrue(Arrays.asList(monitoredHosts()).contains(US1));

		for (String host : monitoredHosts()) {
			monitor.failedHosts.add(host);
			checkDownloads();
			assertEquals(new HashSet<>(Arrays.asList(host)), downtime.keySet());

			// Repeated failures keep the host down; one successful check removes it.
			checkDownloads();
			assertTrue(downtime.containsKey(host));
			monitor.failedHosts.remove(host);
			checkDownloads();
			assertTrue(downtime.isEmpty());
		}
		assertEquals(new HashSet<>(Arrays.asList(monitoredHosts())), monitor.checkedHosts);
	}

	@Test
	public void freeClientFallsBackFromFreeMainToUsMainToMainAndReturnsAfterRecovery() {
		useDowntime();
		country(IPV4, "US");
		assertEquals("freeMain", pool(PredefinedServerSpecialty.MAIN, FREE, IPV4).key);

		monitor.failedHosts.add("dl2.osmand.net");
		checkDownloads();
		assertEquals("freeMain", pool(PredefinedServerSpecialty.MAIN, FREE, IPV4).key);
		assertEquals("dl3.osmand.net", download(PredefinedServerSpecialty.MAIN, FREE, IPV4));

		monitor.failedHosts.add("dl3.osmand.net");
		checkDownloads();
		assertEquals("usMain", pool(PredefinedServerSpecialty.MAIN, FREE, IPV4).key);
		assertEquals(US1, download(PredefinedServerSpecialty.MAIN, FREE, IPV4));

		country(IPV4, "DE"); // usMain is skipped when the country does not match.
		assertEquals("main", pool(PredefinedServerSpecialty.MAIN, FREE, IPV4).key);
		country(IPV4, "US");

		monitor.failedHosts.add(US1);
		checkDownloads();
		assertEquals("main", pool(PredefinedServerSpecialty.MAIN, FREE, IPV4).key);
		assertFalse(monitor.failedHosts.contains(download(PredefinedServerSpecialty.MAIN, FREE, IPV4)));

		monitor.failedHosts.remove(US1);
		checkDownloads();
		assertEquals("usMain", pool(PredefinedServerSpecialty.MAIN, FREE, IPV4).key);
		assertEquals(US1, download(PredefinedServerSpecialty.MAIN, FREE, IPV4));

		monitor.failedHosts.clear();
		checkDownloads();
		assertEquals("freeMain", pool(PredefinedServerSpecialty.MAIN, FREE, IPV4).key);
	}

	@Test
	public void us1DowntimeFallsBackForAllFiveTypesAndUptimeRestoresUsPools() {
		useDowntime();
		country(IPV6, "CA");
		monitor.failedHosts.add(US1);
		checkDownloads();
		assertStandardPools(IPV6);

		monitor.failedHosts.remove(US1);
		checkDownloads();
		assertPool(PredefinedServerSpecialty.MAIN, "usMain", IPV6);
		assertPool(PredefinedServerSpecialty.ROADS, "usRoads", IPV6);
		assertPool(PredefinedServerSpecialty.WIKI, "usWiki", IPV6);
		assertPool(PredefinedServerSpecialty.SRTM, "usSrtm", IPV6);
		assertPool(PredefinedServerSpecialty.HEIGHTMAP, "usHeightmap", IPV6);
	}

	@Test
	public void balancingKeepsOnlyLiveWeightsAndTheirOriginalRatio() {
		useDowntime();
		monitor.failedHosts.addAll(Arrays.asList(monitoredHosts()));
		monitor.failedHosts.remove("dl2.osmand.net");
		monitor.failedHosts.remove("dl8.osmand.net");
		checkDownloads();

		DownloadServerType main = settings.getServerType(PredefinedServerSpecialty.MAIN, "");
		ThreadLocalRandom random = mock(ThreadLocalRandom.class);
		// dl2 has weight 9, dl8 has weight 12: [0..8] -> dl2, [9..20] -> dl8.
		when(random.nextInt(21)).thenReturn(0, 8, 9, 20);
		try (MockedStatic<ThreadLocalRandom> draws = mockStatic(ThreadLocalRandom.class, CALLS_REAL_METHODS)) {
			draws.when(ThreadLocalRandom::current).thenReturn(random);
			assertEquals("dl2.osmand.net", settings.getServer(main, IPV4));
			assertEquals("dl2.osmand.net", settings.getServer(main, IPV4));
			assertEquals("dl8.osmand.net", settings.getServer(main, IPV4));
			assertEquals("dl8.osmand.net", settings.getServer(main, IPV4));
		}
		verify(random, times(4)).nextInt(21);
	}

	@Test
	public void hostFrequenciesFollowWeightsBeforeDuringAndAfterDowntime() {
		useDowntime();
		DownloadServerType main = settings.getServerType(PredefinedServerSpecialty.MAIN, "");
		DownloadServerType freeMain = pool(PredefinedServerSpecialty.MAIN, FREE, IPV4);
		assertDistribution(main);
		assertDistribution(freeMain);

		monitor.failedHosts.add("dl2.osmand.net");
		checkDownloads();
		assertDistribution(main);
		assertDistribution(freeMain);

		monitor.failedHosts.remove("dl2.osmand.net");
		checkDownloads();
		assertDistribution(main);
		assertDistribution(freeMain);
	}

	@Test
	public void aPoolWithOnlyZeroWeightsIsSkipped() {
		country(IPV4, "US");
		settings.servers.get("usMain").put(US1, 0);
		settings.prepare();
		assertPool(PredefinedServerSpecialty.MAIN, "main", IPV4);
	}

	@Test
	public void balancingThrowsWhenTheSelectedPoolsLastHostGoesDown() {
		useDowntime();
		country(IPV4, "US");
		DownloadServerType usMain = pool(PredefinedServerSpecialty.MAIN, PAID, IPV4);

		monitor.failedHosts.add(US1);
		checkDownloads();
		assertUnavailable(() -> settings.getServer(usMain, IPV4));
		assertPool(PredefinedServerSpecialty.MAIN, "main", IPV4);

		monitor.failedHosts.remove(US1);
		checkDownloads();
		assertEquals(US1, settings.getServer(usMain, IPV4));
	}

	@Test
	public void cidrRegionDoesNotFallBackToHostsOutsideItsServerList() {
		useDowntime();
		String ip = "46.80.1.1";
		country(ip, "DE");
		monitor.failedHosts.addAll(Arrays.asList(monitoredHosts()));
		monitor.failedHosts.remove("dl8.osmand.net"); // Alive, but outside de_telekom.
		checkDownloads();
		assertUnavailable(() -> download(PredefinedServerSpecialty.MAIN, PAID, ip));

		monitor.failedHosts.remove("dl4.osmand.net");
		checkDownloads();
		assertEquals("dl4.osmand.net", download(PredefinedServerSpecialty.MAIN, PAID, ip));
	}

	@Test
	public void allHostsDownStopsBalancingAndTheDownloadRequestWith503() throws IOException {
		useDowntime();
		country(IPV4, "US");
		DownloadServerType main = settings.getServerType(PredefinedServerSpecialty.MAIN, "");
		monitor.failedHosts.addAll(Arrays.asList(monitoredHosts()));
		checkDownloads();
		assertUnavailable(() -> settings.getServer(main, IPV4));
		assertUnavailable(() -> download(PredefinedServerSpecialty.MAIN, PAID, IPV4));

		DownloadIndexesService service = mock(DownloadIndexesService.class);
		when(service.getSettings()).thenReturn(settings);
		DownloadIndexController controller = new DownloadIndexController();
		ReflectionTestUtils.setField(controller, "downloadService", service);
		ReflectionTestUtils.setField(controller, "locationService", locationService);
		MockHttpServletRequest request = new MockHttpServletRequest("GET", "/download");
		request.setRemoteAddr(IPV4);
		request.setQueryString("file=Canada_2.obf.zip");
		MockHttpServletResponse response = new MockHttpServletResponse();
		HttpHeaders headers = new HttpHeaders();
		headers.set(HttpHeaders.HOST, "127.0.0.1");
		headers.set(HttpHeaders.USER_AGENT, PAID);
		LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<>();
		params.add("file", "Canada_2.obf.zip");

		try {
			controller.downloadIndex(params, headers, request, response);
			fail("All hosts are down: the request must fail instead of redirecting or serving a file");
		} catch (ResponseStatusException ex) {
			assertEquals(503, ex.getStatusCode().value());
		}
		assertNull(response.getHeader(HttpHeaders.LOCATION));
		assertEquals(0, response.getContentAsByteArray().length);
	}

	@Test
	public void nullStillMeansExplicitSelf() {
		settings.servers.get("main").clear();
		settings.servers.get("main").put(DownloadServerLoadBalancer.SELF, 1);
		settings.prepare();
		DownloadServerType main = settings.getServerType(PredefinedServerSpecialty.MAIN, "");
		assertNull(settings.getServer(main, IPV4));
	}

	private void country(String ip, String code) {
		when(locationService.getField(ip, IpLocationService.COUNTRY_CODE)).thenReturn(code);
	}

	private DownloadServerType pool(PredefinedServerSpecialty specialty, String userAgent, String ip) {
		return settings.getServerType(specialty, userAgent, ip, locationService);
	}

	private String download(PredefinedServerSpecialty specialty, String userAgent, String ip) {
		return settings.getServer(pool(specialty, userAgent, ip), ip);
	}

	private void assertPool(PredefinedServerSpecialty specialty, String key, String ip) {
		DownloadServerType type = pool(specialty, PAID, ip);
		assertEquals(key, type.key);
		String host = settings.getServer(type, ip);
		assertTrue("Host must belong to " + key, settings.servers.get(key).containsKey(host));
		if (key.startsWith("us")) {
			assertEquals(US1, host);
		}
		if (monitor != null) {
			assertFalse("Down host selected: " + host, monitor.failedHosts.contains(host));
		}
	}

	private void assertStandardPools(String ip) {
		assertPool(PredefinedServerSpecialty.MAIN, "main", ip);
		assertPool(PredefinedServerSpecialty.ROADS, "roads", ip);
		assertPool(PredefinedServerSpecialty.WIKI, "wiki", ip);
		assertPool(PredefinedServerSpecialty.SRTM, "srtm", ip);
		assertPool(PredefinedServerSpecialty.HEIGHTMAP, "heightmap", ip);
	}

	private void assertDistribution(DownloadServerType type) {
		int samples = 100_000;
		Map<String, Integer> weights = settings.servers.get(type.key);
		int totalWeight = 0;
		for (String host : weights.keySet()) {
			if (!monitor.failedHosts.contains(host)) {
				totalWeight += weights.get(host);
			}
		}
		Map<String, Integer> counts = new HashMap<>();
		for (int i = 0; i < samples; i++) {
			String host = settings.getServer(type, IPV4);
			counts.put(host, counts.getOrDefault(host, 0) + 1);
		}
		assertTrue(weights.keySet().containsAll(counts.keySet()));
		for (String host : weights.keySet()) {
			double expected = monitor.failedHosts.contains(host) ? 0 : (double) weights.get(host) / totalWeight;
			double actual = (double) counts.getOrDefault(host, 0) / samples;
			// Allow six standard deviations for sampling noise; a down host must have exactly zero hits.
			double tolerance = 6 * Math.sqrt(expected * (1 - expected) / samples);
			assertEquals(type.key + ": " + host, expected, actual, tolerance);
		}
	}

	private void assertUnavailable(Runnable selection) {
		try {
			selection.run();
			fail("No live servers: balancing must throw HTTP 503");
		} catch (ResponseStatusException ex) {
			assertEquals(503, ex.getStatusCode().value());
		}
	}

	// Monitoring owns downServerMap; balancing reads the same map.
	private Map<?, ?> useDowntime() {
		monitor = new DownloadMonitor();
		ReflectionTestUtils.setField(monitor, "config", mock(DatasourceConfiguration.class));
		Map<?, ?> downtime = (Map<?, ?>) ReflectionTestUtils.getField(monitor, "downServerMap");
		assertNotNull("Monitoring must own the downtime map", downtime);
		ReflectionTestUtils.setField(settings, "downServerMap", downtime);
		return downtime;
	}

	private String[] monitoredHosts() {
		return (String[]) ReflectionTestUtils.getField(monitor, "HOSTS_TO_TEST");
	}

	private void checkDownloads() {
		// Check hosts without requesting the map index.
		ReflectionTestUtils.invokeMethod(monitor, "checkDownloads");
	}

	private static class DownloadMonitor extends OsmAndServerMonitorTasks {
		final Set<String> failedHosts = new HashSet<>();
		final Set<String> checkedHosts = new HashSet<>();

		// Replace network I/O, keeping the real status handling.
		@Override
		protected double downloadSpeed(URL url) throws IOException {
			assertTrue("Probe must check the host itself", url.getQuery().contains("self=true"));
			String host = url.getHost();
			checkedHosts.add(host);
			if (failedHosts.contains(host)) {
				throw new IOException("Host is down: " + host);
			}
			return 1;
		}
	}
}
