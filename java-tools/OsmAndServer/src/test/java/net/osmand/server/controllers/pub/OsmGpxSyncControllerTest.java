package net.osmand.server.controllers.pub;

import org.junit.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import net.osmand.server.DatasourceConfiguration;
import net.osmand.server.osmgpx.SyncRemoteOsmGpx;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class OsmGpxSyncControllerTest {

	private MockMvc mvc(String key) {
		return MockMvcBuilders.standaloneSetup(new OsmGpxSyncController(null, new DatasourceConfiguration(), key)).build();
	}

	@Test
	public void requiresConfiguredKeyBeforeAccessingTheDatabase() throws Exception {
		MockMvc mvc = mvc("secret");
		mvc.perform(get(SyncRemoteOsmGpx.API_PATH)).andExpect(status().isForbidden());
		mvc.perform(get(SyncRemoteOsmGpx.API_PATH).header(SyncRemoteOsmGpx.API_KEY_HEADER, "wrong")).andExpect(status().isForbidden());
		mvc("").perform(get(SyncRemoteOsmGpx.API_PATH).header(SyncRemoteOsmGpx.API_KEY_HEADER, "secret")).andExpect(status().isForbidden());
	}

	@Test
	public void validatesCursorAndBatchSize() throws Exception {
		MockMvc mvc = mvc("secret");
		for (String limit : new String[] {"0", Integer.toString(SyncRemoteOsmGpx.MAX_BATCH_SIZE + 1), "invalid"}) {
			mvc.perform(get(SyncRemoteOsmGpx.API_PATH).header(SyncRemoteOsmGpx.API_KEY_HEADER, "secret").param("limit", limit))
					.andExpect(status().isBadRequest());
		}
		mvc.perform(get(SyncRemoteOsmGpx.API_PATH).header(SyncRemoteOsmGpx.API_KEY_HEADER, "secret").param("afterId", "-2"))
				.andExpect(status().isBadRequest());
	}

	@Test
	public void reportsUnavailableDatasourceAfterAuthentication() throws Exception {
		mvc("secret").perform(get(SyncRemoteOsmGpx.API_PATH).header(SyncRemoteOsmGpx.API_KEY_HEADER, "secret"))
				.andExpect(status().isServiceUnavailable());
		mvc("secret").perform(get(SyncRemoteOsmGpx.API_PATH).header(SyncRemoteOsmGpx.API_KEY_HEADER, "secret")
				.param("limit", Integer.toString(SyncRemoteOsmGpx.MAX_BATCH_SIZE)))
				.andExpect(status().isServiceUnavailable());
	}
}
