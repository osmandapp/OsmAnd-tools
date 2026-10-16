package net.osmand.server.api.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.server.api.services.MotdService.DiscountCondition;
import net.osmand.server.api.services.MotdService.MessageParams;
import net.osmand.server.controllers.pub.ApiController;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
public class MotdServiceTest {
	private final ObjectMapper mapper = new ObjectMapper();

	@Test
	public void androidReleaseVersions() throws IOException, ParseException {
		// Version.getFullVersion(): app_name + " " + PackageInfo.versionName.
		// OsmAnd/build.gradle: gplayFree/huawei and gplayFull, default versionName 5.5.0.
		assertClientVersion("OsmAnd 5.5.0", "OsmAnd", "5.5.0", "5.5.0");
		assertClientVersion("OsmAnd+ 5.5.0", "OsmAnd+", "5.5.0", "5.5.0");
	}

	@Test
	public void androidDevelopmentVersions() throws IOException, ParseException {
		// androidFull/nightlyFree; APK_VERSION_SUFFIX uses the documented #99999M format.
		assertClientVersion("OsmAnd~ 5.5.0", "OsmAnd~", "5.5.0", "5.5.0");
		assertClientVersion("OsmAnd~ 5.5.0#99999M", "OsmAnd~", "5.5.0#99999M", "5.5.0");
		assertClientVersion("OsmAnd Nightly 5.5.0", "OsmAnd Nightly", "5.5.0", "5.5.0");
		assertClientVersion("OsmAnd Nightly 5.5.0#99999M", "OsmAnd Nightly", "5.5.0#99999M", "5.5.0");
	}

	@Test
	public void iosReleaseAndTestFlightVersion() throws IOException, ParseException {
		// OADiscountHelper and OAIAPHelper send OAAppVersion.getVersion (CFBundleShortVersionString).
		// OsmAnd.xcodeproj: MARKETING_VERSION=4.7.4; CURRENT_PROJECT_VERSION=4.7.4.2 is not sent.
		assertClientVersion("4.7.4", "", "4.7.4", "4.7.4");
	}

	@Test
	public void reportedMalformedVersionDoesNotMatchRangeConditions() throws IOException, ParseException {
		for (String operator : new String[] {">=", ">", "<=", "<"}) {
			// Reproduce params.version from the production stack trace directly.
			MessageParams params = new MessageParams();
			params.version = "Mobile 6 6";
			assertFalse(matchesVersionCondition(operator + "6.6", params));
			for (MessageParams clientParams : clientParams("OsmAnd Mobile 6 6")) {
				assertFalse(matchesVersionCondition(operator + "6.6", clientParams));
			}
		}
	}

	@Test
	public void missingVersionsDoNotMatchVersionConditions() throws IOException {
		for (String condition : new String[] {">=6.6", ">6.6", "<=6.6", "<6.6", "6.6"}) {
			assertFalse(matches(condition, null));
			assertFalse(matches(condition, ""));
		}
	}

	@Test
	public void numericVersionComparisonsKeepTheirBoundaries() throws IOException {
		assertTrue(matches(">=6.6", "6.6"));
		assertTrue(matches(">=6.6", "6.6.0"));
		assertTrue(matches(">6.6", "6.10"));
		assertFalse(matches(">6.6", "6.6"));
		assertFalse(matches(">=6.6", "6.5"));
		assertTrue(matches("<=6.6", "6.6"));
		assertTrue(matches("<6.6", "6.5"));
		assertFalse(matches("<6.6", "6.6.0"));
		assertFalse(matches("<=6.6", "6.10"));
	}

	@Test
	public void versionPrefixesAndUnrestrictedConditionsStillMatch() throws IOException {
		assertTrue(matches("6.6", "6.6.1"));
		assertFalse(matches("6.6", "6.7"));
		assertTrue(new DiscountCondition().checkCondition(new MessageParams(), null));
	}

	private void assertClientVersion(String fullVersion, String appName, String version, String numericVersion)
			throws IOException, ParseException {
		for (MessageParams params : clientParams(fullVersion)) {
			assertEquals(fullVersion, appName, params.appVersion);
			assertEquals(fullVersion, version, params.version);
			assertTrue(fullVersion, matchesVersionCondition(">=" + numericVersion, params));
			assertTrue(fullVersion, matchesVersionCondition("<=" + numericVersion, params));
			assertFalse(fullVersion, matchesVersionCondition(">" + numericVersion, params));
			assertFalse(fullVersion, matchesVersionCondition("<" + numericVersion, params));
		}
	}

	private MessageParams[] clientParams(String fullVersion) throws IOException, ParseException {
		CapturingMotdService service = new CapturingMotdService();
		ApiController controller = new ApiController();
		ReflectionTestUtils.setField(controller, "motdService", service);
		HttpHeaders headers = new HttpHeaders();
		MockHttpServletRequest request = new MockHttpServletRequest();
		controller.getMessage(fullVersion, null, null, null, null, null, null, null, headers, request);
		MessageParams motdParams = service.params;
		controller.getActiveSubscriptions(fullVersion, null, null, null, null, null, null, headers, request);
		return new MessageParams[] {motdParams, service.params};
	}

	private boolean matches(String condition, String version) throws IOException {
		MessageParams params = new MessageParams();
		params.version = version;
		return matchesVersionCondition(condition, params);
	}

	private boolean matchesVersionCondition(String condition, MessageParams params) throws IOException {
		DiscountCondition discountCondition = mapper.readValue(
				"{\"version\":" + mapper.writeValueAsString(condition) + "}", DiscountCondition.class);
		return discountCondition.checkCondition(params, null);
	}

	private static class CapturingMotdService extends MotdService {
		private MessageParams params;

		@Override
		public HashMap<String, Object> getMessage(MessageParams params) {
			this.params = params;
			return null;
		}

		@Override
		public File getSubscriptions(MessageParams params) {
			this.params = params;
			return new File("empty.json");
		}
	}
}
