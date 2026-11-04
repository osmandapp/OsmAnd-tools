package net.osmand.server.api.services;

import io.sigpipe.jbsdiff.Diff;
import io.sigpipe.jbsdiff.InvalidHeaderException;
import net.osmand.apkdelta.ApkArchive;
import net.osmand.apkdelta.ApkDeltaFormat;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * Builds a delta from what a phone has (its manifest) to an APK here, in the format of
 * {@link ApkDeltaFormat}. An entry the phone has unchanged is copied on the phone. An entry it has
 * in another version gets a binary patch, if a build here still holds that version. Anything else
 * is sent whole.
 *
 * Patches of entries are kept on disk: everybody who updates to the same build from the same
 * version shares them, and building them is the slow part.
 */
public class ApkDeltaBuilder {

	private static final Log LOGGER = LogFactory.getLog(ApkDeltaBuilder.class);

	/** Below this a patch with its three compressed streams does not pay for itself. */
	private static final int MIN_ENTRY_TO_PATCH = 4 * 1024;
	/** The phone keeps the patch of an entry in memory; a larger entry is sent whole, as a stream. */
	private static final int MAX_PATCH_SIZE = 16 * 1024 * 1024;

	// measured on weekly pairs of nightly builds
	private static final double DEFLATED_PATCH_RATIO = 0.5;
	private static final double STORED_PATCH_RATIO = 0.05;
	private static final int METADATA_PER_ENTRY = 24;

	public record Result(long size, int copied, int patched, int full) {
	}

	private interface Payload {
		void writeTo(OutputStream output) throws IOException;
	}

	private final File target;
	private final Map<String, ApkArchive.Entry> phoneEntries;
	private final Map<String, File> oldBuilds;

	/** @param oldBuilds a build that holds the entry, by {@link ApkArchive.Entry#contentKey()} */
	public ApkDeltaBuilder(File target, Map<String, ApkArchive.Entry> phoneEntries, Map<String, File> oldBuilds) {
		this.target = target;
		this.phoneEntries = phoneEntries;
		this.oldBuilds = oldBuilds;
	}

	/** Finds the builds with what the phone has, the first of the builds first. */
	public static Map<String, File> findOldBuilds(Map<String, ApkArchive.Entry> phoneEntries, List<File> builds) {
		Set<String> wanted = phoneEntries.values().stream().map(ApkArchive.Entry::contentKey).collect(Collectors.toSet());
		Map<String, File> found = new HashMap<>();
		for (File build : builds) {
			if (found.size() == wanted.size()) {
				break;
			}
			try {
				for (ApkArchive.Entry entry : ApkArchive.readEntries(build)) {
					if (wanted.contains(entry.contentKey())) {
						found.putIfAbsent(entry.contentKey(), build);
					}
				}
			} catch (IOException e) {
				LOGGER.warn("Skipping the unreadable build " + build.getName() + ": " + e.getMessage());
			}
		}
		return found;
	}

	/**
	 * The size of the delta without building it, with an average size in place of each patch;
	 * 0 if the phone already has this build.
	 */
	public long estimate() throws IOException {
		double size = 0;
		boolean changed = false;
		for (ApkArchive.Entry entry : ApkArchive.readEntries(target)) {
			size += METADATA_PER_ENTRY;
			byte kind = kind(entry);
			changed |= kind != ApkDeltaFormat.KIND_COPY;
			if (kind == ApkDeltaFormat.KIND_FULL) {
				size += entry.compressedSize;
			} else if (kind == ApkDeltaFormat.KIND_DELTA) {
				size += entry.compressedSize * (entry.method == ZipArchiveEntry.DEFLATED ? DEFLATED_PATCH_RATIO : STORED_PATCH_RATIO);
			}
		}
		return changed ? (long) size : 0;
	}

	private byte kind(ApkArchive.Entry entry) {
		ApkArchive.Entry phoneEntry = phoneEntries.get(entry.name);
		if (phoneEntry != null && entry.isSameAs(phoneEntry)) {
			return ApkDeltaFormat.KIND_COPY;
		}
		boolean patched = phoneEntry != null && oldBuilds.containsKey(phoneEntry.contentKey())
				&& entry.compressedSize >= MIN_ENTRY_TO_PATCH;
		return patched ? ApkDeltaFormat.KIND_DELTA : ApkDeltaFormat.KIND_FULL;
	}

	public Result build(File entryPatches, File output) throws IOException {
		ByteArrayOutputStream container = new ByteArrayOutputStream();
		ByteArrayOutputStream table = new ByteArrayOutputStream();
		DataOutputStream tableOutput = new DataOutputStream(table);
		List<Payload> payload = new ArrayList<>();
		int copied = 0;
		int patched = 0;
		int full = 0;
		try (ZipFile zip = new ZipFile(target); RandomAccessFile file = new RandomAccessFile(target, "r")) {
			List<ZipArchiveEntry> entries = Collections.list(zip.getEntriesInPhysicalOrder());
			tableOutput.writeInt(entries.size());
			long position = 0;
			for (ZipArchiveEntry zipEntry : entries) {
				int headerLength = Math.toIntExact(zipEntry.getDataOffset() - position);
				container.write(read(file, position, headerLength));
				position = zipEntry.getDataOffset() + zipEntry.getCompressedSize();
				tableOutput.writeInt(headerLength);
				ApkArchive.Entry entry = new ApkArchive.Entry(zipEntry.getName(), zipEntry.getCrc(), zipEntry.getSize(),
						zipEntry.getCompressedSize(), zipEntry.getMethod());
				byte kind = kind(entry);
				File patch = kind == ApkDeltaFormat.KIND_DELTA ? patch(zip, zipEntry, entry, entryPatches) : null;
				if (kind == ApkDeltaFormat.KIND_COPY) {
					tableOutput.writeByte(ApkDeltaFormat.KIND_COPY);
					tableOutput.writeUTF(entry.name);
					copied++;
				} else if (patch != null && patch.length() < Math.min(entry.compressedSize, MAX_PATCH_SIZE)) {
					tableOutput.writeByte(ApkDeltaFormat.KIND_DELTA);
					tableOutput.writeUTF(entry.name);
					tableOutput.writeByte(entry.method);
					tableOutput.writeLong(patch.length());
					payload.add(stream -> Files.copy(patch.toPath(), stream));
					patched++;
				} else {
					tableOutput.writeByte(ApkDeltaFormat.KIND_FULL);
					tableOutput.writeLong(entry.compressedSize);
					payload.add(stream -> zip.getRawInputStream(zipEntry).transferTo(stream));
					full++;
				}
			}
			int tailLength = Math.toIntExact(file.length() - position);
			container.write(read(file, position, tailLength));
			tableOutput.writeInt(tailLength);

			ByteArrayOutputStream metadata = new ByteArrayOutputStream();
			try (DataOutputStream metadataOutput = new DataOutputStream(new BZip2CompressorOutputStream(metadata))) {
				metadataOutput.writeInt(container.size());
				container.writeTo(metadataOutput);
				table.writeTo(metadataOutput);
			}
			try (DataOutputStream delta = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(output)))) {
				delta.write(ApkDeltaFormat.MAGIC);
				// the file that is open: a new build can replace the one at this path at any moment
				file.seek(0);
				delta.write(DigestUtils.sha256(Channels.newInputStream(file.getChannel())));
				delta.writeLong(metadata.size());
				metadata.writeTo(delta);
				for (Payload part : payload) {
					part.writeTo(delta);
				}
			}
		}
		return new Result(output.length(), copied, patched, full);
	}

	/** The patch of uncompressed bytes, or null if the phone could not get the same APK bytes from it. */
	private File patch(ZipFile zip, ZipArchiveEntry zipEntry, ApkArchive.Entry entry, File entryPatches) throws IOException {
		ApkArchive.Entry phoneEntry = phoneEntries.get(entry.name);
		String name = DigestUtils.sha1Hex(entry.contentKey() + "<" + phoneEntry.contentKey() + ":"
				+ entry.compressedSize + ":" + entry.method);
		File patch = new File(entryPatches, name + ".bsdiff");
		if (!patch.exists()) {
			byte[] oldContent = oldContent(phoneEntry);
			if (oldContent == null) {
				return null;
			}
			byte[] content = readAll(zip.getInputStream(zipEntry));
			File temp = File.createTempFile(name, ".tmp", entryPatches);
			try {
				// a file left empty remembers the answer
				if (entry.method == ZipArchiveEntry.STORED
						|| Arrays.equals(deflate(content), readAll(zip.getRawInputStream(zipEntry)))) {
					try (OutputStream output = new BufferedOutputStream(new FileOutputStream(temp))) {
						Diff.diff(oldContent, content, output);
					} catch (CompressorException | InvalidHeaderException e) {
						throw new IOException(e);
					}
				}
				Files.move(temp.toPath(), patch.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			} finally {
				temp.delete();
			}
		}
		return patch.length() == 0 ? null : patch;
	}

	/** What the phone has, or null if the build is gone or was replaced since it was found. */
	private byte[] oldContent(ApkArchive.Entry phoneEntry) {
		try (java.util.zip.ZipFile zip = new java.util.zip.ZipFile(oldBuilds.get(phoneEntry.contentKey()));
		     InputStream input = zip.getInputStream(zip.getEntry(phoneEntry.name))) {
			byte[] content = input.readAllBytes();
			CRC32 crc = new CRC32();
			crc.update(content);
			return crc.getValue() == phoneEntry.crc ? content : null;
		} catch (IOException | RuntimeException e) {
			LOGGER.warn("Old version of " + phoneEntry.name + " is not available: " + e.getMessage());
			return null;
		}
	}

	private static byte[] readAll(InputStream stream) throws IOException {
		try (InputStream input = stream) {
			return input.readAllBytes();
		}
	}

	private static byte[] deflate(byte[] content) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream(content.length / 2);
		Deflater deflater = new Deflater(ApkDeltaFormat.DEFLATE_LEVEL, true);
		try (OutputStream output = new DeflaterOutputStream(bytes, deflater)) {
			output.write(content);
		} finally {
			deflater.end();
		}
		return bytes.toByteArray();
	}

	private static byte[] read(RandomAccessFile file, long offset, int length) throws IOException {
		byte[] bytes = new byte[length];
		file.seek(offset);
		file.readFully(bytes);
		return bytes;
	}
}
