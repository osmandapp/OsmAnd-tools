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
import java.nio.file.Files;
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

	private static final String BUILD = "latest-night-build/OsmAnd-test.apk";

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	private MockMvc mvc;
	private BuildDeltaService service;
	private File root;
	private File gen;

	@Before
	public void setUp() throws Exception {
		root = temp.newFolder();
		new File(root, "latest-night-build").mkdirs();
		new File(root, "secret").mkdirs();
		byte[] payload = new byte[2_000_000];
		new Random(1).nextBytes(payload);
		zip(new File(root, BUILD), payload);
		zip(new File(root, "secret/x.apk"), payload);
		service = new BuildDeltaService();
		ReflectionTestUtils.setField(service, "filesLocation", root.getPath());
		gen = temp.newFolder();
		ReflectionTestUtils.setField(service, "genLocation", gen.getPath());
		BuildDeltaController controller = new BuildDeltaController();
		ReflectionTestUtils.setField(controller, "deltaService", service);
		mvc = MockMvcBuilders.standaloneSetup(controller).build();
	}

	@Test
	public void keepsTheLastBuildsOfEachKindAndRemovesTheRest() throws Exception {
		File published = new File(root, "published/night-builds");
		published.mkdirs();
		File list = new File(root, "published/night-builds-json/list.json");
		list.getParentFile().mkdirs();
		ReflectionTestUtils.setField(service, "nightBuildsUrl", list.toURI().toString());
		publish(published, list, 1, 12);
		assertFalse(service.isSupported());

		service.syncOldBuilds();
		assertTrue(service.isSupported());
		File[] kept = new File(gen, "apk-old-builds").listFiles();
		assertEquals(10, kept.length);
		assertFalse(new File(gen, "apk-old-builds/OsmAnd-default-nb-02.apk").exists());
		assertTrue(new File(gen, "apk-old-builds/OsmAnd-default-nb-12.apk").exists());
		assertFalse(new File(gen, "apk-old-builds/OsmAnd-map-sample.apk").exists());

		publish(published, list, 5, 14);
		service.syncOldBuilds();
		assertFalse(new File(gen, "apk-old-builds/OsmAnd-default-nb-04.apk").exists());
		assertTrue(new File(gen, "apk-old-builds/OsmAnd-default-nb-14.apk").exists());
	}

	private static void publish(File folder, File list, int firstDay, int lastDay) throws Exception {
		StringBuilder json = new StringBuilder("[{\"name\":\"OsmAnd-map-sample.apk\",\"size\":1,\"mtime\":\"Mon, 02 Nov 2026 12:00:00 GMT\"}");
		Files.write(new File(folder, "OsmAnd-map-sample.apk").toPath(), new byte[1]);
		for (int day = firstDay; day <= lastDay; day++) {
			String name = String.format("OsmAnd-default-nb-%02d.apk", day);
			Files.write(new File(folder, name).toPath(), new byte[day]);
			json.append(String.format(",{\"name\":\"%s\",\"size\":%d,\"mtime\":\"Sun, %02d Nov 2026 12:00:00 GMT\"}", name, day, day));
		}
		Files.writeString(list.toPath(), json.append("]").toString());
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
