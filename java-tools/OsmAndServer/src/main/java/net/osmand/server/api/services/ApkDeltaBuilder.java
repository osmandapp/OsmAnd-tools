package net.osmand.server.api.services;

import io.sigpipe.jbsdiff.Diff;
import io.sigpipe.jbsdiff.InvalidHeaderException;
import net.osmand.apkdelta.ApkArchive;
import net.osmand.apkdelta.ApkDeltaFormat;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Builds a delta from what a phone has (its manifest) to an APK here, in the format of
 * {@link ApkDeltaFormat}. An entry the phone has unchanged is copied on the phone. An entry it has
 * in an older version gets a binary patch, if a build here still holds that version. Anything else
 * is sent whole.
 *
 * Patches of entries are kept on disk: everybody who updates to the same build from the same
 * version shares them, and building them is the slow part.
 */
public class ApkDeltaBuilder {

	/** Below this a patch with its three compressed streams does not pay for itself. */
	static final int MIN_ENTRY_TO_PATCH = 4 * 1024;

	private static final String BUILD_EXTENSION = ".apk";

	// measured on weekly pairs of nightly builds, see estimate()
	private static final double DEFLATED_PATCH_RATIO = 0.5;
	private static final double STORED_PATCH_RATIO = 0.05;
	private static final int METADATA_PER_ENTRY = 24;

	public record OldEntry(File apk, ApkArchive.Entry entry) {
	}

	public record Result(long size, long targetSize, int copied, int patched, int full) {
	}

	private record Operation(ApkArchive.Entry entry, long dataOffset, byte kind, ApkArchive.Entry oldEntry,
	                         Future<File> patch) {
	}

	private final File entryPatches;
	private final ExecutorService executor;

	public ApkDeltaBuilder(File entryPatches, ExecutorService executor) {
		this.entryPatches = entryPatches;
		this.executor = executor;
	}

	/** Finds the old versions of the entries in builds, the newest build first. */
	public static Map<String, OldEntry> findOldEntries(File target, List<ApkArchive.Entry> manifest, List<File> builds)
			throws IOException {
		List<ApkArchive.Entry> targetEntries;
		try (RandomAccessFile file = new RandomAccessFile(target, "r")) {
			targetEntries = ApkArchive.readEntries(file);
		}
		Map<String, ApkArchive.Entry> phoneEntries = byName(manifest);
		Map<String, ApkArchive.Entry> wanted = new HashMap<>();
		for (ApkArchive.Entry entry : targetEntries) {
			ApkArchive.Entry phoneEntry = phoneEntries.get(entry.name);
			if (phoneEntry != null && !entry.isSameAs(phoneEntry)) {
				wanted.put(phoneEntry.contentKey(), phoneEntry);
			}
		}
		Map<String, OldEntry> found = new HashMap<>();
		for (File build : builds) {
			if (found.size() == wanted.size()) {
				break;
			}
			if (!build.getName().endsWith(BUILD_EXTENSION)) {
				continue;
			}
			try (RandomAccessFile file = new RandomAccessFile(build, "r")) {
				for (ApkArchive.Entry entry : ApkArchive.readEntries(file)) {
					String key = entry.contentKey();
					if (wanted.containsKey(key)) {
						found.putIfAbsent(key, new OldEntry(build, entry));
					}
				}
			}
		}
		return found;
	}

	/**
	 * The size of a delta without building it: the same decisions as {@link #build}, with an
	 * average patch size in place of real patches. Only a whole entry is known exactly.
	 */
	public static long estimate(File target, List<ApkArchive.Entry> manifest, Map<String, OldEntry> oldEntries)
			throws IOException {
		Map<String, ApkArchive.Entry> phoneEntries = byName(manifest);
		List<ApkArchive.Entry> entries;
		try (RandomAccessFile file = new RandomAccessFile(target, "r")) {
			entries = ApkArchive.readEntries(file);
		}
		double size = (double) entries.size() * METADATA_PER_ENTRY;
		for (ApkArchive.Entry entry : entries) {
			ApkArchive.Entry phoneEntry = phoneEntries.get(entry.name);
			if (phoneEntry != null && entry.isSameAs(phoneEntry)) {
				continue;
			}
			boolean patched = phoneEntry != null && oldEntries.containsKey(phoneEntry.contentKey())
					&& entry.compressedSize >= MIN_ENTRY_TO_PATCH;
			double ratio = !patched ? 1 : entry.method == ApkArchive.DEFLATED ? DEFLATED_PATCH_RATIO : STORED_PATCH_RATIO;
			size += entry.compressedSize * ratio;
		}
		return (long) size;
	}

	public Result build(File target, List<ApkArchive.Entry> manifest, Map<String, OldEntry> oldEntries, File output)
			throws IOException {
		Map<String, ApkArchive.Entry> phoneEntries = byName(manifest);
		try (RandomAccessFile file = new RandomAccessFile(target, "r")) {
			List<ApkArchive.Entry> entries = new ArrayList<>(ApkArchive.readEntries(file));
			entries.sort(Comparator.comparingLong(entry -> entry.headerOffset));
			List<Operation> operations = new ArrayList<>(entries.size());
			for (ApkArchive.Entry entry : entries) {
				ApkArchive.Entry phoneEntry = phoneEntries.get(entry.name);
				long dataOffset = ApkArchive.dataOffset(file, entry);
				if (phoneEntry != null && entry.isSameAs(phoneEntry)) {
					operations.add(new Operation(entry, dataOffset, ApkDeltaFormat.KIND_COPY, phoneEntry, null));
					continue;
				}
				OldEntry oldEntry = phoneEntry == null ? null : oldEntries.get(phoneEntry.contentKey());
				if (oldEntry != null && entry.compressedSize >= MIN_ENTRY_TO_PATCH) {
					operations.add(new Operation(entry, dataOffset, ApkDeltaFormat.KIND_DELTA, phoneEntry,
							executor.submit(() -> patch(target, entry, dataOffset, oldEntry))));
				} else {
					operations.add(new Operation(entry, dataOffset, ApkDeltaFormat.KIND_FULL, null, null));
				}
			}
			return write(target, file, operations, output);
		}
	}

	private Result write(File target, RandomAccessFile file, List<Operation> operations, File output) throws IOException {
		ByteArrayOutputStream container = new ByteArrayOutputStream();
		ByteArrayOutputStream table = new ByteArrayOutputStream();
		DataOutputStream tableOutput = new DataOutputStream(table);
		List<Object[]> payload = new ArrayList<>();
		long payloadSize = 0;
		int copied = 0;
		int patched = 0;
		int full = 0;
		long position = 0;
		tableOutput.writeInt(operations.size());
		for (Operation operation : operations) {
			int headerLength = Math.toIntExact(operation.dataOffset - position);
			container.write(read(file, position, headerLength));
			position = operation.dataOffset + operation.entry.compressedSize;
			tableOutput.writeInt(headerLength);
			File patch = waitFor(operation.patch);
			ApkArchive.Entry entry = operation.entry;
			if (operation.kind == ApkDeltaFormat.KIND_COPY) {
				tableOutput.writeByte(ApkDeltaFormat.KIND_COPY);
				tableOutput.writeUTF(entry.name);
				copied++;
			} else if (patch != null && patch.length() < entry.compressedSize) {
				tableOutput.writeByte(ApkDeltaFormat.KIND_DELTA);
				tableOutput.writeUTF(operation.oldEntry.name);
				tableOutput.writeByte(entry.method);
				tableOutput.writeLong(entry.size);
				tableOutput.writeLong(entry.compressedSize);
				tableOutput.writeLong(crc(read(file, operation.dataOffset, Math.toIntExact(entry.compressedSize))));
				tableOutput.writeLong(payloadSize);
				tableOutput.writeLong(patch.length());
				payload.add(new Object[] {patch});
				payloadSize += patch.length();
				patched++;
			} else {
				tableOutput.writeByte(ApkDeltaFormat.KIND_FULL);
				tableOutput.writeLong(payloadSize);
				tableOutput.writeLong(entry.compressedSize);
				payload.add(new Object[] {operation.dataOffset, entry.compressedSize});
				payloadSize += entry.compressedSize;
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
		try (DataOutputStream deltaOutput = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(output)))) {
			deltaOutput.write(ApkDeltaFormat.MAGIC);
			deltaOutput.writeLong(file.length());
			deltaOutput.write(sha256(target));
			deltaOutput.writeLong(metadata.size());
			metadata.writeTo(deltaOutput);
			for (Object[] part : payload) {
				if (part[0] instanceof File patch) {
					Files.copy(patch.toPath(), deltaOutput);
				} else {
					copy(file, (Long) part[0], (Long) part[1], deltaOutput);
				}
			}
		}
		return new Result(output.length(), file.length(), copied, patched, full);
	}

	/** The patch of uncompressed bytes, or null if the phone could not get the same APK bytes from it. */
	private File patch(File target, ApkArchive.Entry entry, long dataOffset, OldEntry oldEntry) throws IOException {
		File patch = new File(entryPatches, patchName(entry, oldEntry.entry));
		if (patch.exists()) {
			return patch.length() == 0 ? null : patch;
		}
		byte[] compressed;
		try (RandomAccessFile file = new RandomAccessFile(target, "r")) {
			compressed = read(file, dataOffset, Math.toIntExact(entry.compressedSize));
		}
		byte[] content = entry.method == ApkArchive.DEFLATED ? inflate(compressed, entry.size) : compressed;
		File temp = File.createTempFile(patch.getName(), ".tmp", entryPatches);
		try {
			if (entry.method == ApkArchive.DEFLATED && !isReproducible(content, compressed)) {
				// an empty file remembers the answer
				Files.write(temp.toPath(), new byte[0]);
			} else {
				byte[] oldContent = readContent(oldEntry);
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
		return patch.length() == 0 ? null : patch;
	}

	private static String patchName(ApkArchive.Entry entry, ApkArchive.Entry oldEntry) {
		String key = entry.contentKey() + "<" + oldEntry.crc + ":" + oldEntry.size + ":" + entry.compressedSize
				+ ":" + entry.method;
		return hex(digest("SHA-1").digest(key.getBytes(StandardCharsets.UTF_8))) + ".bsdiff";
	}

	private static byte[] readContent(OldEntry oldEntry) throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(oldEntry.apk, "r")) {
			ApkArchive.Entry entry = oldEntry.entry;
			byte[] bytes = read(file, ApkArchive.dataOffset(file, entry), Math.toIntExact(entry.compressedSize));
			return entry.method == ApkArchive.DEFLATED ? inflate(bytes, entry.size) : bytes;
		}
	}

	private static boolean isReproducible(byte[] content, byte[] compressed) {
		Deflater deflater = new Deflater(ApkDeltaFormat.DEFLATE_LEVEL, true);
		try {
			deflater.setInput(content);
			deflater.finish();
			byte[] buffer = new byte[64 * 1024];
			int position = 0;
			while (!deflater.finished()) {
				int length = deflater.deflate(buffer);
				if (position + length > compressed.length
						|| !Arrays.equals(buffer, 0, length, compressed, position, position + length)) {
					return false;
				}
				position += length;
			}
			return position == compressed.length;
		} finally {
			deflater.end();
		}
	}

	private static byte[] inflate(byte[] compressed, long size) throws IOException {
		byte[] content = new byte[Math.toIntExact(size)];
		Inflater inflater = new Inflater(true);
		try {
			inflater.setInput(compressed);
			int position = 0;
			while (position < content.length && !inflater.finished()) {
				int length = inflater.inflate(content, position, content.length - position);
				if (length == 0 && inflater.needsInput()) {
					break;
				}
				position += length;
			}
			if (position != content.length) {
				throw new IOException("Broken entry");
			}
			return content;
		} catch (java.util.zip.DataFormatException e) {
			throw new IOException(e);
		} finally {
			inflater.end();
		}
	}

	private static Map<String, ApkArchive.Entry> byName(List<ApkArchive.Entry> entries) {
		Map<String, ApkArchive.Entry> map = new HashMap<>();
		for (ApkArchive.Entry entry : entries) {
			map.put(entry.name, entry);
		}
		return map;
	}

	private static File waitFor(Future<File> patch) throws IOException {
		if (patch == null) {
			return null;
		}
		try {
			return patch.get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException(e);
		} catch (ExecutionException e) {
			throw new IOException(e.getCause());
		}
	}

	private static byte[] read(RandomAccessFile file, long offset, int length) throws IOException {
		byte[] bytes = new byte[length];
		file.seek(offset);
		file.readFully(bytes);
		return bytes;
	}

	private static void copy(RandomAccessFile file, long offset, long length, OutputStream output) throws IOException {
		byte[] buffer = new byte[64 * 1024];
		file.seek(offset);
		long left = length;
		while (left > 0) {
			int read = file.read(buffer, 0, (int) Math.min(left, buffer.length));
			if (read < 0) {
				throw new IOException("Unexpected end of file");
			}
			output.write(buffer, 0, read);
			left -= read;
		}
	}

	private static long crc(byte[] bytes) {
		CRC32 crc = new CRC32();
		crc.update(bytes);
		return crc.getValue();
	}

	private static byte[] sha256(File file) throws IOException {
		MessageDigest digest = digest("SHA-256");
		try (InputStream input = new DigestInputStream(new FileInputStream(file), digest)) {
			input.transferTo(OutputStream.nullOutputStream());
		}
		return digest.digest();
	}

	private static MessageDigest digest(String algorithm) {
		try {
			return MessageDigest.getInstance(algorithm);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	private static String hex(byte[] bytes) {
		return HexFormat.of().formatHex(bytes);
	}
}
