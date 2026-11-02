package net.osmand.server.controllers.pub;

import net.osmand.apkdelta.ApkArchive;
import net.osmand.server.api.services.BuildDeltaService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class BuildDeltaControllerTest {

	private static final String BUILD = "night-builds/OsmAnd-test-nb.apk";

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	private MockMvc mvc;
	private File root;
	private byte[] payload;

	@Before
	public void setUp() throws Exception {
		root = temp.newFolder();
		new File(root, "night-builds").mkdirs();
		new File(root, "secret").mkdirs();
		payload = new byte[2_000_000];
		new Random(1).nextBytes(payload);
		zip(new File(root, BUILD), payload);
		zip(new File(root, "secret/x.apk"), payload);
		BuildDeltaService service = new BuildDeltaService();
		ReflectionTestUtils.setField(service, "filesLocation", root.getPath());
		ReflectionTestUtils.setField(service, "genLocation", temp.newFolder().getPath());
		BuildDeltaController controller = new BuildDeltaController();
		ReflectionTestUtils.setField(controller, "deltaService", service);
		mvc = MockMvcBuilders.standaloneSetup(controller).build();
	}

	@Test
	public void deltaOfTheSameApkIsDownloaded() throws Exception {
		String json = mvc.perform(post("/api/builds/delta").param("path", BUILD).content(manifest(new File(root, BUILD))))
				.andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
		String key = json.replaceAll(".*\"key\":\"([0-9a-f]+)\".*", "$1");
		byte[] delta = mvc.perform(get("/api/builds/delta/" + key)).andExpect(status().isOk())
				.andReturn().getResponse().getContentAsByteArray();
		assertEquals(json.contains("\"size\":" + delta.length), true);
	}

	@Test
	public void sizesAreAnsweredAtOnceAndBecomeExact() throws Exception {
		byte[] manifest = manifest(new File(root, BUILD));
		String first = mvc.perform(post("/api/builds/delta/sizes").content(manifest))
				.andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
		assertTrue(first, first.contains("\"path\":\"" + BUILD + "\""));
		assertFalse(first, first.contains("secret"));
		// the delta asked for is the one the sizes were about
		mvc.perform(post("/api/builds/delta").param("path", BUILD).content(manifest)).andExpect(status().isOk());
		String second = mvc.perform(post("/api/builds/delta/sizes").content(manifest))
				.andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
		assertTrue(second, second.contains("\"exact\":true"));
	}

	@Test
	public void brokenManifestIsARequestError() throws Exception {
		byte[] garbage = {1, 2, 3};
		mvc.perform(post("/api/builds/delta/sizes").content(garbage)).andExpect(status().isBadRequest());
		mvc.perform(post("/api/builds/delta").param("path", BUILD).content(garbage)).andExpect(status().isBadRequest());
	}

	@Test
	public void noSizesForAPhoneWithAnotherApp() throws Exception {
		File other = temp.newFile("other.apk");
		zip(other, new byte[10]);
		String sizes = mvc.perform(post("/api/builds/delta/sizes").content(manifest(other)))
				.andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
		assertEquals("[]", sizes);
	}

	@Test
	public void wholeApkIsBetterForAPhoneWithAnotherApp() throws Exception {
		File other = temp.newFile("other.apk");
		byte[] bytes = new byte[10];
		zip(other, bytes);
		mvc.perform(post("/api/builds/delta").param("path", BUILD).content(manifest(other)))
				.andExpect(status().isNoContent());
	}

	@Test
	public void onlyBuildFoldersAreServed() throws Exception {
		byte[] manifest = manifest(new File(root, BUILD));
		for (String path : List.of("secret/x.apk", "night-builds/../secret/x.apk", "/etc/passwd", "night-builds/none.apk")) {
			mvc.perform(post("/api/builds/delta").param("path", path).content(manifest)).andExpect(status().isNotFound());
		}
		mvc.perform(get("/api/builds/delta/0000000000000000000000000000000000000000")).andExpect(status().isNotFound());
		mvc.perform(get("/api/builds/delta/..%2F..%2Fsecret")).andExpect(status().isNotFound());
	}

	private static byte[] manifest(File apk) throws Exception {
		try (RandomAccessFile file = new RandomAccessFile(apk, "r")) {
			return ApkArchive.writeManifest(ApkArchive.readEntries(file));
		}
	}

	private static void zip(File file, byte[] bytes) throws Exception {
		try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(file))) {
			zip.putNextEntry(new ZipEntry("classes.dex"));
			zip.write(bytes);
			zip.closeEntry();
		}
	}
}
