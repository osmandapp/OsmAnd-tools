package net.osmand.server.api.services;

import net.osmand.apkdelta.ApkArchive;
import net.osmand.apkdelta.ApkDeltaApplier;
import net.osmand.apkdelta.ApkDeltaFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ApkDeltaBuilderTest {

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	private byte[] stored;
	private byte[] deflated;
	private byte[] unchanged;

	@Before
	public void setUp() {
		Random random = new Random(1);
		stored = new byte[300_000];
		random.nextBytes(stored);
		deflated = "the same text again and again ".repeat(20_000).getBytes(StandardCharsets.UTF_8);
		unchanged = new byte[50_000];
		random.nextBytes(unchanged);
	}

	@Test
	public void buildsTheSameBytesFromAnOlderBuild() throws Exception {
		File oldApk = apk("old.apk", stored, deflated, null);
		File newApk = apk("new.apk", changed(stored), changed(deflated), new byte[] {1, 2, 3});
		File delta = delta(oldApk, newApk, List.of(oldApk));
		Assert.assertTrue("delta " + delta.length(), delta.length() < newApk.length() / 4);
		assertRebuilt(oldApk, delta, newApk);
	}

	@Test
	public void sendsEntriesWholeIfTheServerHasNoOldBuild() throws Exception {
		File oldApk = apk("old.apk", stored, deflated, null);
		File newApk = apk("new.apk", changed(stored), changed(deflated), null);
		assertRebuilt(oldApk, delta(oldApk, newApk, List.of()), newApk);
	}

	@Test
	public void skipsAnUnreadableBuild() throws Exception {
		File oldApk = apk("old.apk", stored, deflated, null);
		File newApk = apk("new.apk", changed(stored), changed(deflated), null);
		File broken = temp.newFile("broken.apk");
		assertRebuilt(oldApk, delta(oldApk, newApk, List.of(broken, oldApk)), newApk);
	}

	@Test
	public void sendsAnEntryWholeIfItsOldBuildWasReplaced() throws Exception {
		File phoneApk = apk("phone.apk", stored, deflated, null);
		File newApk = apk("new.apk", changed(stored), changed(deflated), null);
		File serverCopy = temp.newFile();
		Files.copy(phoneApk.toPath(), serverCopy.toPath(), StandardCopyOption.REPLACE_EXISTING);
		Map<String, ApkArchive.Entry> manifest = manifest(phoneApk);
		Map<String, File> oldBuilds = ApkDeltaBuilder.findOldBuilds(manifest, List.of(serverCopy));
		// a nightly replaced under the same name while the delta waited in the queue
		Files.copy(newApk.toPath(), serverCopy.toPath(), StandardCopyOption.REPLACE_EXISTING);
		File delta = temp.newFile();
		ApkDeltaBuilder.Result result = new ApkDeltaBuilder(newApk, manifest, oldBuilds).build(temp.newFolder(), delta);
		Assert.assertEquals(0, result.patched());
		assertRebuilt(phoneApk, delta, newApk);
	}

	@Test(expected = EOFException.class)
	public void aShortDeltaIsNotAMismatch() throws Exception {
		File oldApk = apk("old.apk", stored, deflated, null);
		File delta = delta(oldApk, apk("new.apk", changed(stored), changed(deflated), new byte[100_000]), List.of());
		byte[] bytes = Files.readAllBytes(delta.toPath());
		Files.write(delta.toPath(), Arrays.copyOf(bytes, bytes.length - 40));
		ApkDeltaApplier.apply(oldApk, delta, temp.newFile("out.apk"), temp.newFolder(), () -> false);
	}

	@Test(expected = IOException.class)
	public void aBrokenDeltaIsAnIOException() throws Exception {
		File oldApk = apk("old.apk", stored, deflated, null);
		File delta = delta(oldApk, apk("new.apk", changed(stored), changed(deflated), null), List.of(oldApk));
		byte[] bytes = Files.readAllBytes(delta.toPath());
		bytes[ApkDeltaFormat.MAGIC.length + ApkDeltaFormat.SHA256_LENGTH] = (byte) 0xff;
		Files.write(delta.toPath(), bytes);
		ApkDeltaApplier.apply(oldApk, delta, temp.newFile("out.apk"), temp.newFolder(), () -> false);
	}

	@Test(expected = ApkDeltaApplier.MismatchException.class)
	public void refusesAnotherOldApk() throws Exception {
		File oldApk = apk("old.apk", stored, deflated, null);
		File newApk = apk("new.apk", changed(stored), changed(deflated), null);
		File delta = delta(oldApk, newApk, List.of(oldApk));
		byte[] otherLib = stored.clone();
		new Random(2).nextBytes(otherLib);
		File other = apk("other.apk", otherLib, deflated, null);
		ApkDeltaApplier.apply(other, delta, temp.newFile("out.apk"), temp.newFolder(), () -> false);
	}

	private File delta(File oldApk, File newApk, List<File> builds) throws IOException {
		Map<String, ApkArchive.Entry> manifest = manifest(oldApk);
		File delta = temp.newFile();
		new ApkDeltaBuilder(newApk, manifest, ApkDeltaBuilder.findOldBuilds(manifest, builds)).build(temp.newFolder(), delta);
		return delta;
	}

	private static Map<String, ApkArchive.Entry> manifest(File apk) throws IOException {
		return BuildDeltaService.readManifest(new ByteArrayInputStream(ApkArchive.writeManifest(ApkArchive.readEntries(apk))));
	}

	private void assertRebuilt(File oldApk, File delta, File newApk) throws IOException {
		File result = temp.newFile();
		ApkDeltaApplier.apply(oldApk, delta, result, temp.newFolder(), () -> false);
		Assert.assertArrayEquals(Files.readAllBytes(newApk.toPath()), Files.readAllBytes(result.toPath()));
	}

	private static byte[] changed(byte[] bytes) {
		byte[] copy = Arrays.copyOf(bytes, bytes.length + 1000);
		for (int i = 0; i < 500; i++) {
			copy[1000 + i * 37] ^= 0x55;
		}
		return copy;
	}

	private File apk(String name, byte[] lib, byte[] text, byte[] added) throws IOException {
		File file = temp.newFile(name);
		try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(file))) {
			zip.setLevel(Deflater.DEFAULT_COMPRESSION);
			store(zip, "lib/stored.so", lib);
			store(zip, "assets/unchanged.bin", unchanged);
			zip.putNextEntry(new ZipEntry("classes.dex"));
			zip.write(text);
			zip.closeEntry();
			if (added != null) {
				store(zip, "assets/added.bin", added);
			}
		}
		return file;
	}

	private static void store(ZipOutputStream zip, String name, byte[] bytes) throws IOException {
		ZipEntry entry = new ZipEntry(name);
		CRC32 crc = new CRC32();
		crc.update(bytes);
		entry.setMethod(ZipEntry.STORED);
		entry.setSize(bytes.length);
		entry.setCompressedSize(bytes.length);
		entry.setCrc(crc.getValue());
		zip.putNextEntry(entry);
		zip.write(bytes);
		zip.closeEntry();
	}
}
