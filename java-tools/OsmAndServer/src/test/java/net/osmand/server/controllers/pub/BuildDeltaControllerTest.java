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
	private File phoneApk;
	private byte[] payload;

	@Before
	public void setUp() throws Exception {
		root = temp.newFolder();
		new File(root, "latest-night-build").mkdirs();
		new File(root, "secret").mkdirs();
		payload = new byte[30_000_000];
		new Random(1).nextBytes(payload);
		zip(new File(root, BUILD), payload);
		zip(new File(root, "secret/x.apk"), payload);
		phoneApk = temp.newFile("phone.apk");
		zip(phoneApk, payload, "changed.bin", new byte[] {1, 2, 3});
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
		File list = publishedList(published);
		publish(published, list, 1, 12);

		service.syncOldBuilds();
		assertEquals(5, new File(gen, "apk-old-builds").listFiles().length);
		assertFalse(new File(gen, "apk-old-builds/OsmAnd-default-nb-07.apk").exists());
		assertTrue(new File(gen, "apk-old-builds/OsmAnd-default-nb-12.apk").exists());
		assertFalse(new File(gen, "apk-old-builds/OsmAnd-map-sample.apk").exists());

		publish(published, list, 5, 14);
		service.syncOldBuilds();
		assertFalse(new File(gen, "apk-old-builds/OsmAnd-default-nb-09.apk").exists());
		assertTrue(new File(gen, "apk-old-builds/OsmAnd-default-nb-14.apk").exists());
	}

	@Test
	public void offersOnlyTheBuildsItHasAndBuildsOnlyTheChosenOne() throws Exception {
		File old = new File(gen, "apk-old-builds/OsmAnd-default-nb-01.apk");
		old.getParentFile().mkdirs();
		Files.copy(new File(root, BUILD).toPath(), old.toPath());
		String keptPath = "night-builds/OsmAnd-default-nb-01.apk";
		byte[] manifest = manifest(phoneApk);
		String sizes = mvc.perform(post("/api/builds/delta/sizes").param("path", BUILD, keptPath, "night-builds/none.apk")
						.content(manifest)).andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
		assertTrue(sizes, sizes.contains(BUILD) && sizes.contains(keptPath));
		assertFalse(sizes, sizes.contains("none.apk"));
		assertFalse(sizes, sizes.contains("\"exact\":true"));
		Thread.sleep(300);
		assertFalse(new File(gen, "apk-delta").exists());
		mvc.perform(post("/api/builds/delta").param("path", keptPath).content(manifest)).andExpect(status().isOk());
	}

	@Test
	public void onlyAPhoneAskingStartsTheDownloadOfOldBuilds() throws Exception {
		File published = new File(root, "published/night-builds");
		publish(published, publishedList(published), 1, 2);
		File oldBuilds = new File(gen, "apk-old-builds");
		mvc.perform(post("/api/builds/delta").param("path", BUILD).content(manifest(phoneApk)))
				.andExpect(status().isOk());
		mvc.perform(post("/api/builds/delta/sizes").param("path", BUILD).content(new byte[] {1}))
				.andExpect(status().isBadRequest());
		assertFalse(oldBuilds.exists());

		mvc.perform(post("/api/builds/delta/sizes").param("path", BUILD).content(manifest(new File(root, BUILD))))
				.andExpect(status().isOk());
		for (int i = 0; i < 100 && !new File(oldBuilds, "OsmAnd-default-nb-02.apk").exists(); i++) {
			Thread.sleep(50);
		}
		assertTrue(new File(oldBuilds, "OsmAnd-default-nb-02.apk").exists());
	}

	private File publishedList(File published) {
		published.mkdirs();
		File list = new File(root, "published/night-builds-json/list.json");
		list.getParentFile().mkdirs();
		ReflectionTestUtils.setField(service, "nightBuildsUrl", list.toURI().toString());
		return list;
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
		byte[] delta = mvc.perform(post("/api/builds/delta").param("path", BUILD).content(manifest(phoneApk)))
				.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();
		assertArrayEquals(ApkDeltaFormat.MAGIC, Arrays.copyOf(delta, ApkDeltaFormat.MAGIC.length));
	}

	@Test
	public void sizesAreAnsweredAtOnceAndBecomeExact() throws Exception {
		byte[] manifest = manifest(phoneApk);
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
	public void offersAllLatestBuildsAndTheNewestOldOnes() throws Exception {
		File oldBuilds = new File(gen, "apk-old-builds");
		oldBuilds.mkdirs();
		for (int day = 1; day <= 9; day++) {
			File build = new File(oldBuilds, "OsmAnd-default-nb-0" + day + ".apk");
			Files.copy(new File(root, BUILD).toPath(), build.toPath());
			build.setLastModified(day * 86_400_000L);
		}
		String sizes = mvc.perform(post("/api/builds/delta/sizes").content(manifest(phoneApk)))
				.andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
		assertTrue(sizes, sizes.contains(BUILD));
		assertTrue(sizes, sizes.contains("night-builds/OsmAnd-default-nb-02.apk"));
		assertTrue(sizes, sizes.contains("night-builds/OsmAnd-default-nb-09.apk"));
		assertFalse(sizes, sizes.contains("night-builds/OsmAnd-default-nb-01.apk"));
		assertFalse(sizes, sizes.contains("secret"));
	}

	@Test
	public void noOfferForTheInstalledBuildOrASmallSaving() throws Exception {
		byte[] installed = manifest(new File(root, BUILD));
		mvc.perform(post("/api/builds/delta").param("path", BUILD).content(installed)).andExpect(status().isNoContent());
		String sizes = mvc.perform(post("/api/builds/delta/sizes").param("path", BUILD).content(installed))
				.andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
		assertEquals("[]", sizes);

		String small = "latest-night-build/OsmAnd-small.apk";
		zip(new File(root, small), new byte[1_000_000]);
		File smallPhone = temp.newFile("small-phone.apk");
		zip(smallPhone, new byte[1_000_000], "changed.bin", new byte[] {1});
		sizes = mvc.perform(post("/api/builds/delta/sizes").param("path", small).content(manifest(smallPhone)))
				.andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
		assertEquals("[]", sizes);
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
		zip(file, bytes, "changed.bin", new byte[] {4, 5, 6, 7});
	}

	private static void zip(File file, byte[] bytes, String name, byte[] small) throws Exception {
		try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(file))) {
			zip.putNextEntry(new ZipEntry("classes.dex"));
			zip.write(bytes);
			zip.closeEntry();
			zip.putNextEntry(new ZipEntry(name));
			zip.write(small);
			zip.closeEntry();
		}
	}
}
