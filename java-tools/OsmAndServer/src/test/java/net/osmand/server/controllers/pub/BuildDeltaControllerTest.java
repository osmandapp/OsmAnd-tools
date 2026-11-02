package net.osmand.server.controllers.pub;

import net.osmand.apkdelta.ApkArchive;
import net.osmand.apkdelta.ApkDeltaFormat;
import net.osmand.server.api.services.BuildDeltaService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class BuildDeltaControllerTest {

	private static final String BUILD = "night-builds/OsmAnd-test-nb.apk";

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	private MockMvc mvc;
	private BuildDeltaService service;
	private File root;

	@Before
	public void setUp() throws Exception {
		root = temp.newFolder();
		new File(root, "night-builds").mkdirs();
		new File(root, "secret").mkdirs();
		byte[] payload = new byte[2_000_000];
		new Random(1).nextBytes(payload);
		zip(new File(root, BUILD), payload);
		zip(new File(root, "secret/x.apk"), payload);
		service = new BuildDeltaService();
		ReflectionTestUtils.setField(service, "filesLocation", root.getPath());
		ReflectionTestUtils.setField(service, "genLocation", temp.newFolder().getPath());
		BuildDeltaController controller = new BuildDeltaController();
		ReflectionTestUtils.setField(controller, "deltaService", service);
		mvc = MockMvcBuilders.standaloneSetup(controller).build();
	}

	@Test
	public void supportedOnlyWithOldBuilds() {
		assertTrue(service.isSupported());
		new File(root, BUILD).delete();
		assertFalse(service.isSupported());
	}

	@Test
	public void deltaIsReturned() throws Exception {
		byte[] delta = mvc.perform(post("/api/builds/delta").param("path", BUILD).content(manifest(new File(root, BUILD))))
				.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();
		assertArrayEquals(ApkDeltaFormat.MAGIC, Arrays.copyOf(delta, ApkDeltaFormat.MAGIC.length));
	}

	@Test
	public void sizesAreAnsweredAtOnceAndBecomeExact() throws Exception {
		byte[] manifest = manifest(new File(root, BUILD));
		String first = mvc.perform(post("/api/builds/delta/sizes").param("path", BUILD, "secret/x.apk").content(manifest))
				.andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
		assertTrue(first, first.contains("\"path\":\"" + BUILD + "\""));
		assertFalse(first, first.contains("secret"));
		mvc.perform(post("/api/builds/delta").param("path", BUILD).content(manifest)).andExpect(status().isOk());
		String second = mvc.perform(post("/api/builds/delta/sizes").param("path", BUILD).content(manifest))
				.andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
		assertTrue(second, second.contains("\"exact\":true"));
	}

	@Test
	public void wholeApkIsBetterForAPhoneWithAnotherApp() throws Exception {
		File other = temp.newFile("other.apk");
		zip(other, new byte[10]);
		mvc.perform(post("/api/builds/delta").param("path", BUILD).content(manifest(other)))
				.andExpect(status().isNoContent());
		String sizes = mvc.perform(post("/api/builds/delta/sizes").param("path", BUILD).content(manifest(other)))
				.andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
		assertEquals("[]", sizes);
	}

	@Test
	public void brokenManifestsAreRequestErrors() throws Exception {
		ByteArrayOutputStream bomb = new ByteArrayOutputStream();
		try (GZIPOutputStream gzip = new GZIPOutputStream(bomb)) {
			gzip.write(new byte[8 * 1024 * 1024]);
		}
		String twice = "1 1 1 8 same\n1 1 1 8 same\n";
		ByteArrayOutputStream duplicate = new ByteArrayOutputStream();
		try (GZIPOutputStream gzip = new GZIPOutputStream(duplicate)) {
			gzip.write(twice.getBytes());
		}
		for (byte[] manifest : List.of(new byte[] {1, 2, 3}, bomb.toByteArray(), duplicate.toByteArray())) {
			mvc.perform(post("/api/builds/delta/sizes").param("path", BUILD).content(manifest)).andExpect(status().isBadRequest());
			mvc.perform(post("/api/builds/delta").param("path", BUILD).content(manifest)).andExpect(status().isBadRequest());
		}
	}

	@Test
	public void onlyBuildFoldersAreServed() throws Exception {
		byte[] manifest = manifest(new File(root, BUILD));
		for (String path : List.of("secret/x.apk", "night-builds/../secret/x.apk", "/etc/passwd", "night-builds/none.apk")) {
			mvc.perform(post("/api/builds/delta").param("path", path).content(manifest)).andExpect(status().isNotFound());
		}
	}

	private static byte[] manifest(File apk) throws Exception {
		return ApkArchive.writeManifest(ApkArchive.readEntries(apk));
	}

	private static void zip(File file, byte[] bytes) throws Exception {
		try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(file))) {
			zip.putNextEntry(new ZipEntry("classes.dex"));
			zip.write(bytes);
			zip.closeEntry();
		}
	}
}
