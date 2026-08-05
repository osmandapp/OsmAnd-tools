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
	static final int TEST_USER_ID = -1; // set user-id or use -1 to all user from S3
	public static final int THREAD_POOL = 8;
	public static final String INFO_GZ_SUFFIX = ".info.gz";
	public static final String GPX_INFO_GZ_SUFFIX = ".gpx.info.gz";
	public static String BUCKET = "prod-user-data";

	public static void main(String[] args) throws Exception {

		String endpoint = System.getenv("S3_ENDPOINT_URL");
		String region = System.getenv("S3_REGION");
		if (endpoint == null || region == null) {
			throw new IllegalStateException("Missing S3_ENDPOINT_URL or S3_REGION env vars");
		}
		S3Client s3 = S3Client.builder()
				.endpointOverride(URI.create(endpoint))
				.region(Region.of(region))
				.build();
		LOG.info("Start processing ...");
		ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL);
		AtomicInteger submittedFiles = new AtomicInteger(0);
		AtomicInteger fixedFiles = new AtomicInteger(0);
		AtomicInteger processedUsers = new AtomicInteger(0);
		AtomicInteger lastPrinted = new AtomicInteger(0);
		try {
			if (TEST_USER_ID < 0) {
				List<CommonPrefix> users = getUsers(s3, BUCKET);
				int totalUsers = users.size();
				for (CommonPrefix userPrefix : users) {
					processUser(userPrefix.prefix(), s3, submittedFiles, pool, fixedFiles, processedUsers, totalUsers,
							lastPrinted);
				}
			} else {
				String userPrefix = "user-" + TEST_USER_ID + "/";
				processUser(userPrefix, s3, submittedFiles, pool, fixedFiles, processedUsers, 1,
						lastPrinted);
			}
		} finally {
			pool.shutdown();
			pool.awaitTermination(1, TimeUnit.DAYS);
			s3.close();
			LOG.info(String.format("Done! Processed %d users %d info files %d fixed files.",
					processedUsers.get(), submittedFiles.get(), fixedFiles.get()));
		}
	}

	private static void processUser(String userPrefix, S3Client s3, AtomicInteger submittedFiles,
	                                ExecutorService pool, AtomicInteger fixedFiles, AtomicInteger processedUsers,
	                                int totalUsers, AtomicInteger lastPrinted) throws InterruptedException {
		String gpxPrefix = userPrefix + "GPX/";
		List<S3Object> files;
		try {
			files = getFiles(s3, BUCKET, gpxPrefix);
		} catch (Exception e) {
			LOG.error(String.format("SKIP prefix %s -> %s", gpxPrefix, e.getMessage()));
			return;
		}
		List<Future<?>> userFutures = new ArrayList<>();
		for (S3Object obj : files) {
			String key = obj.key();
			if (!key.endsWith(GPX_INFO_GZ_SUFFIX)) {
				continue;
			}
			submittedFiles.incrementAndGet();
			userFutures.add(pool.submit(() -> {
				try {
					boolean fixed = processFile(s3, BUCKET, key);
					if (fixed) {
						fixedFiles.incrementAndGet();
					}
				} catch (Exception e) {
					LOG.info("FAILED: " + key + " -> " + e.getMessage());
				}
			}));
		}
		// Wait for ALL this user's file tasks to finish
		for (Future<?> f : userFutures) {
			try {
				f.get();
			} catch (ExecutionException e) {
				LOG.error("Task error: " + e.getCause());
			}
		}

		int done = processedUsers.incrementAndGet();
		int milestone = done * 100 / totalUsers;
		int lastMilestone = lastPrinted.get();
		if (milestone > lastMilestone && lastPrinted.compareAndSet(lastMilestone, milestone)) {
			LOG.info(String.format("Progress: %d%% (%d/%d users) %d info files %d fixed.",
					milestone, done, totalUsers, submittedFiles.get(), fixedFiles.get()));
		}
	}

	static boolean processFile(S3Client s3, String bucket, String key) throws Exception {
		String baseName = baseNameFromKey(key);
		File tmpIn = Files.createTempFile("info-json-in-", ".gz").toFile();
		File tmpOut = Files.createTempFile("info-json-out-", ".gz").toFile();

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
				LOG.info("Updated: s3://" + bucket + "/" + key + " (would upload)");
			} else {
				upload(s3, bucket, key, tmpOut);
				LOG.info("Updated: s3://" + bucket + "/" + key + " (uploaded)");
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
				LOG.info(String.format("Pages: %d, Users: %d", pageCount, totalUser));
			}
		}
		LOG.info(String.format("Total pages: %d, total users: %d", pageCount, result.size()));
		return result;
	}

	static List<S3Object> getFiles(S3Client s3, String bucket, String prefix) {
		ListObjectsV2Request req = ListObjectsV2Request.builder()
				.bucket(bucket)
				.prefix(prefix)
				.encodingType("url")
				.build();
		List<S3Object> files = new ArrayList<>();
		for (ListObjectsV2Response page : s3.listObjectsV2Paginator(req)) {
			files.addAll(page.contents());
		}
		return files;
	}

	// user-0001/GPX/1755163505312-123-track.gpx.info.gz -> 123-track.gpx
	static String baseNameFromKey(String key) {
		String gpxName = key.substring(key.lastIndexOf('/') + 1);
		gpxName = gpxName.substring(0, gpxName.length() - INFO_GZ_SUFFIX.length());
		return gpxName.replaceFirst("^\\d+-", "");
	}
}
