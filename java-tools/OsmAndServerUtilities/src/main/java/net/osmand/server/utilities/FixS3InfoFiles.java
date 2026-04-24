package net.osmand.server.utilities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class FixS3InfoFiles {

	static final ObjectMapper MAPPER = new ObjectMapper();
	static final boolean TEST_RUN = true; // set false to enable real upload
	public static final int THREAD_POOL = 8;
	public static final String INFO_GZ_SUFFIX = ".info.gz";
	public static final String GPX_INFO_GZ_SUFFIX = ".gpx.info.gz";
	public static String BUCKET = "prod-user-data";

	public static void main(String[] args) throws Exception {

		try (S3Client s3 = S3Client.create()) {
			ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL);

			for (CommonPrefix userPrefix : getUsers(s3, BUCKET)) {
				String gpxPrefix = userPrefix.prefix() + "GPX/";
				System.out.println("Listing: s3://" + BUCKET + "/" + gpxPrefix);

				for (S3Object obj : getFiles(s3, BUCKET, gpxPrefix)) {
					String key = obj.key();
					if (!key.endsWith(GPX_INFO_GZ_SUFFIX)) {
						continue;
					}
					System.out.println(key);
					pool.submit(() -> {
//						try {
//							processOne(s3, BUCKET, key);
//						} catch (Exception e) {
//							System.err.println("FAILED: " + key + " -> " + e.getMessage());
//						}
					});
				}
			}

			pool.shutdown();
			pool.awaitTermination(1, TimeUnit.DAYS);
			System.out.println("All done.");
		}
	}

	static void processOne(S3Client s3, String bucket, String key) throws Exception {
		String baseName = baseNameFromKey(key);
		System.out.println("Processing: s3://" + bucket + "/" + key + " -> file=" + baseName);

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

			TransformResult tr = fixIfBroken(root, baseName);
			if (!tr.changed) {
				System.out.println("OK (not broken): s3://" + bucket + "/" + key + " (skip)");
				return;
			}

			try (OutputStream fout = new BufferedOutputStream(new FileOutputStream(tmpOut));
			     GZIPOutputStream gzOut = new GZIPOutputStream(fout);
			     Writer w = new OutputStreamWriter(gzOut, StandardCharsets.UTF_8)) {
				MAPPER.writeValue(w, tr.node);
			}

			if (TEST_RUN) {
				System.out.println("DEST: s3://" + bucket + "/" + key);
				System.out.println("FILE: " + tmpOut.getAbsolutePath());
				System.out.println("Updated: s3://" + bucket + "/" + key + " (would upload)");
			} else {
				//	upload(s3, bucket, key, tmpOut);
				System.out.println("Updated: s3://" + bucket + "/" + key + " (uploaded)");
			}

		} finally {
			tmpIn.delete();
			tmpOut.delete();
		}
	}

	static TransformResult fixIfBroken(JsonNode node, String baseName) {
		if (node == null || !node.isObject()) {
			return new TransformResult(node, false);
		}

		ObjectNode obj = (ObjectNode) node;
		if (obj.size() == 1 && obj.has("pointsGroups")) {
			ObjectNode out = MAPPER.createObjectNode();
			out.put("type", "GPX");
			out.put("file", "/tracks/" + baseName);
			out.put("subtype", "gpx");
			out.setAll(obj);
			return new TransformResult(out, true);
		}
		return new TransformResult(node, false);
	}

	record TransformResult(JsonNode node, boolean changed) {
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
			if (pageCount % 1000 == 0) {
				System.out.printf("Pages: %d, Users: %d%n", pageCount, totalUser);
			}
		}
		System.out.println("Total users" + result.size());

		return result;
	}

	static Iterable<S3Object> getFiles(S3Client s3, String bucket, String prefix) {
		ListObjectsV2Request req = ListObjectsV2Request.builder()
				.bucket(bucket)
				.prefix(prefix)
				.build();
		return () -> s3.listObjectsV2Paginator(req).contents().iterator();
	}

	static String baseNameFromKey(String key) {
		String name = key.substring(key.lastIndexOf('/') + 1);
		name = name.substring(0, name.length() - INFO_GZ_SUFFIX.length());
		return name.replaceFirst("^\\d+-", "");
	}
}
