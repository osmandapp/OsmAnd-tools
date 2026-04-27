package net.osmand.server.utilities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class FixS3InfoFiles {
	static final Log LOG = LogFactory.getLog(FixS3InfoFiles.class);

	static final ObjectMapper MAPPER = new ObjectMapper();
	static final boolean TEST_RUN = true; // set false to enable real upload
	public static final int THREAD_POOL = 8;
	public static final String INFO_GZ_SUFFIX = ".info.gz";
	public static final String GPX_INFO_GZ_SUFFIX = ".gpx.info.gz";
	public static String BUCKET = "prod-user-data";

	public static void main(String[] args) throws Exception {

		try (S3Client s3 = S3Client.builder()
				.endpointOverride(URI.create("https://s3.nl-ams.scw.cloud"))
				.region(Region.of("nl-ams"))
				.build()) {
			LOG.info("Start processing ...");
			ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL);
			AtomicInteger submittedFiles = new AtomicInteger(0);
			AtomicInteger fixedFiles = new AtomicInteger(0);
			AtomicInteger processedUsers = new AtomicInteger(0);
			AtomicInteger lastPrinted = new AtomicInteger(0);
			List<CommonPrefix> users = getUsers(s3, BUCKET);
			int totalUsers = users.size();
			for (CommonPrefix userPrefix : users) {
				String gpxPrefix = userPrefix.prefix() + "GPX/";
				int done = processedUsers.incrementAndGet();
				for (S3Object obj : getFiles(s3, BUCKET, gpxPrefix)) {
					String key = obj.key();
					if (!key.endsWith(GPX_INFO_GZ_SUFFIX)) {
						continue;
					}
					submittedFiles.incrementAndGet();
					pool.submit(() -> {
						try {
							boolean fixed = processOne(s3, BUCKET, key);
							if (fixed) {
								fixedFiles.incrementAndGet();
							}
						} catch (Exception e) {
							System.err.println("FAILED: " + key + " -> " + e.getMessage());
						}
					});
				}
				int milestone = done * 100 / totalUsers;
				int last = lastPrinted.get();
				if (milestone > last && lastPrinted.compareAndSet(last, milestone)) {
					LOG.info(String.format("Progress: %d%% (%d/%d users) %d info files %d fixed\n",
							milestone, done, totalUsers, submittedFiles.get(), fixedFiles.get()));
				}
			}

			pool.shutdown();
			pool.awaitTermination(1, TimeUnit.DAYS);
			System.out.printf("\nDone! Processed %d users %d info files %d fixed files.\n",
					totalUsers, submittedFiles.get(), fixedFiles.get());
		}
	}

	static boolean processOne(S3Client s3, String bucket, String key) throws Exception {
		String baseName = baseNameFromKey(key);
		File tmpIn = Files.createTempFile("gpx-in-", ".gz").toFile();
		File tmpOut = Files.createTempFile("gpx-out-", ".gz").toFile();

		try {
			try (ResponseInputStream<GetObjectResponse> in = s3.getObject(
					GetObjectRequest.builder().bucket(bucket).key(key).build());
			     OutputStream out = new BufferedOutputStream(new FileOutputStream(tmpIn))) {
				in.transferTo(out);
			}

			JsonNode root;
			try (InputStream fin = new BufferedInputStream(new FileInputStream(tmpIn));
			     GZIPInputStream gin = new GZIPInputStream(fin)) {
				root = MAPPER.readTree(gin);
			}

			ProcessedFile processedFile = fixIfBroken(root, baseName);
			if (!processedFile.changed) {
				return false;
			}

			try (OutputStream fout = new BufferedOutputStream(new FileOutputStream(tmpOut));
			     GZIPOutputStream gzOut = new GZIPOutputStream(fout);
			     Writer w = new OutputStreamWriter(gzOut, StandardCharsets.UTF_8)) {
				MAPPER.writeValue(w, processedFile.node);
			}

			if (TEST_RUN) {
				System.out.println("DEST: s3://" + bucket + "/" + key);
				System.out.println("FILE: " + tmpOut.getAbsolutePath());
				System.out.println("Updated: s3://" + bucket + "/" + key + " (would upload)");
			} else {
				//	upload(s3, bucket, key, tmpOut); comment for test!!!
				System.out.println("Updated: s3://" + bucket + "/" + key + " (uploaded)");
			}
			return true;
		} finally {
			tmpIn.delete();
			tmpOut.delete();
		}
	}

	static ProcessedFile fixIfBroken(JsonNode node, String baseName) {
		if (node == null || !node.isObject()) {
			return new ProcessedFile(node, false);
		}

		ObjectNode obj = (ObjectNode) node;
		if (obj.size() == 1 && obj.has("pointsGroups")) {
			ObjectNode out = MAPPER.createObjectNode();
			out.put("type", "GPX");
			out.put("file", "/tracks/" + baseName);
			out.put("subtype", "gpx");
			out.setAll(obj);
			return new ProcessedFile(out, true);
		}
		return new ProcessedFile(node, false);
	}

	record ProcessedFile(JsonNode node, boolean changed) {
	}

	static void upload(S3Client s3, String bucket, String key, File file) {
		s3.putObject(
				PutObjectRequest.builder().bucket(bucket).key(key).build(),
				RequestBody.fromFile(file)
		);
	}

	static List<CommonPrefix> getUsers(S3Client s3, String bucket) {
		ListObjectsV2Request req = ListObjectsV2Request.builder()
				.bucket(bucket)
				.delimiter("/")
				.encodingType("url")
				.build();

		List<CommonPrefix> result = new ArrayList<>();
		int pageCount = 0;
		int totalUser = 0;

		for (ListObjectsV2Response page : s3.listObjectsV2Paginator(req)) {
			pageCount++;
			List<CommonPrefix> prefixes = page.commonPrefixes();
			if (prefixes != null) {
				result.addAll(prefixes);
				totalUser += prefixes.size();
			}
			if (pageCount % 20 == 0) {
				System.out.printf("Pages: %d, Users: %d%n", pageCount, totalUser);
			}
		}
		System.out.printf("Total pages: %d, total users: %d\n", pageCount, result.size());
		return result;
	}

	static Iterable<S3Object> getFiles(S3Client s3, String bucket, String prefix) {
		ListObjectsV2Request req = ListObjectsV2Request.builder()
				.bucket(bucket)
				.prefix(prefix)
				.encodingType("url")
				.build();
		return () -> s3.listObjectsV2Paginator(req).contents().iterator();
	}

	static String baseNameFromKey(String key) {
		String name = key.substring(key.lastIndexOf('/') + 1);
		name = name.substring(0, name.length() - INFO_GZ_SUFFIX.length());
		return name.replaceFirst("^\\d+-", "");
	}
}
