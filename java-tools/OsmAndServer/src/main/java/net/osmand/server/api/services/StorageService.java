package net.osmand.server.api.services;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.amazonaws.services.s3.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import net.osmand.server.api.services.DownloadIndexesService.ServerCommonFile;
import net.osmand.util.Algorithms;
import org.springframework.web.multipart.MultipartFile;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

@Service
public class StorageService {

	private static final String FILE_SEPARATOR = "/";
	private static final String GPX = "gpx";
	private static final String GPX_GZ = "gpx.gz";
	
	protected static final Log LOGGER = LogFactory.getLog(StorageService.class);

	protected static final String LOCAL_STORAGE = "local";

	@Value("${storage.default}")
	private String defaultStorage;
	
	private long reconnectTime = 0;

	@Autowired
	private Environment env;

	private Map<String, StorageType> storageProviders = new ConcurrentHashMap<>();

	private List<StorageType> defaultStorageProviders;

	public String getDefaultStorage() {
		getAndInitDefaultStorageProviders();
		return defaultStorage;
	}

	protected List<StorageType> getAndInitDefaultStorageProviders() {
		if (defaultStorageProviders == null) {
			defaultStorage = defaultStorage.trim();
			if (Algorithms.isEmpty(defaultStorage)) {
				defaultStorage = LOCAL_STORAGE;
			}
			String[] split = defaultStorage.split(",");
			List<StorageType> sProviders = new ArrayList<StorageService.StorageType>();
			for (String s : split) {
				sProviders.add(getStorageProviderById(s));
			}
			defaultStorageProviders = sProviders;
		}
		return defaultStorageProviders;
	}

	public boolean hasStorageProviderById(String id) {
		getAndInitDefaultStorageProviders();
		return getStorageProviderById(id) != null;
	}
	
	private StorageType getStorageProviderById(String id) {
		id = id.trim();
		StorageType st = storageProviders.get(id);
		if (st != null) {
			return st;
		}
		if (id.equals(LOCAL_STORAGE)) {
			st = new StorageType();
			st.local = true;
		} else {
			String prefix = "storage.datasource." + id + ".";
			String endpointUrl = env.getProperty(prefix + "endpoint");
			checkNotNull(endpointUrl, "endpoint", id);
			String region = env.getProperty(prefix + "region");
			checkNotNull(region, "endpoint", id);
			String bucket = env.getProperty(prefix + "bucket");
			checkNotNull(region, "bucket", id);
			String accessKey = env.getProperty(prefix + "accesskey");
			String secretKey = env.getProperty(prefix + "secretkey");
			AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
					.withEndpointConfiguration(new EndpointConfiguration(endpointUrl, region));
			if (!Algorithms.isEmpty(accessKey)) {
				builder = builder.withCredentials(
						new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
			}
			LOGGER.info(String.format("Configure %s with %s in %s bucket=%s: accesskey=%s, secretKeyLength=%d", id, endpointUrl,
					region, bucket, accessKey, secretKey == null ? 0 : secretKey.length()));
			AmazonS3 s3 = builder.build();
			st = new StorageType();
			st.s3ConnBuilder = builder;
			st.bucket = bucket;
			st.s3Conn = s3;
		}
		storageProviders.put(id, st);
		return st;
	}

	private void checkNotNull(String vl, String name, String id) {
		if (Algorithms.isEmpty(vl)) {
			String msg = String.format("For storage configuration '%s' %s was not specified in application properties",
					id, name);
			LOGGER.warn(msg);
			throw new IllegalArgumentException(msg);
		}
	}

	public boolean storeLocally() {
		for (StorageType s : getAndInitDefaultStorageProviders()) {
			if (s.local) {
				return true;
			}
		}
		return false;
	}
	
	public void remapFileNames(String storage, String userFolder, String oldStorageFileName, String newStorageFileName) {
		if (!Algorithms.isEmpty(storage) && !newStorageFileName.trim().equals(oldStorageFileName.trim())) {
			String oldKey = userFolder + FILE_SEPARATOR + oldStorageFileName;
			String newKey = userFolder + FILE_SEPARATOR + newStorageFileName;
			for (String id : storage.split(",")) {
				StorageType toStore = getStorageProviderById(id);
				try {
					if (toStore != null && !toStore.local && toStore.s3Conn.doesObjectExist(toStore.bucket, oldKey)) {
						CopyObjectResult res = toStore.s3Conn.copyObject(toStore.bucket, oldKey, toStore.bucket,
								newKey);
						if (res.getLastModifiedDate() != null) {
							toStore.s3Conn.deleteObject(toStore.bucket, oldKey);
						}
					}
				} catch (com.amazonaws.SdkClientException e) {
					handleException(toStore, "remap", userFolder, oldStorageFileName, e);
					throw e;
				}
			}
		}
	}
	
	public String backupData(String storageId, String fld, String storageFileName, String storage, byte[] data) throws IOException {
		if (!Algorithms.isEmpty(storage)) {
			for (String id : storage.split(",")) {
				if (storageId.equals(id)) {
					// already stored
					return null;
				}
			}
		}
		StorageType toStore = getStorageProviderById(storageId);
		InputStream is = null;
		if (data != null && data.length > 0) {
			is = new ByteArrayInputStream(data);
		}
		if (!Algorithms.isEmpty(storage) && is == null) {
			for (String id : storage.split(",")) {
				if (!storageId.equals(id)) {
					is = getFileInputStream(id, fld, storageFileName);
					if (is != null) {
						break;
					}
				}
			}
		}
		if(is == null) {
			throw new IllegalStateException(String.format("Impossible to retrieve file %s/%s", fld, storageFileName));
		}
		saveFile(fld, storageFileName, toStore, is, data.length);
		is.close();
		String nstorage = storage == null ? LOCAL_STORAGE : storage;
		nstorage += "," + storageId;
		return nstorage; 
	}
	
	
	public String save(String fld, String fileName, @Valid @NotNull @NotEmpty InternalZipFile file) throws IOException {
		for (StorageType s : getAndInitDefaultStorageProviders()) {
			InputStream is = file.getInputStream();
			saveFile(fld, fileName, s, is, file.getSize());
			is.close();
		}
		return defaultStorage;
	}
	

	private void saveFile(String fld, String fileName, StorageType s, InputStream is, long fileSize) {
		if (!s.local) {
			ObjectMetadata om = new ObjectMetadata();
			om.setContentLength(fileSize);
			try {
				s.s3Conn.putObject(s.bucket, fld + FILE_SEPARATOR + fileName, is, om);
			} catch (com.amazonaws.SdkClientException e) {
				handleException(s, "save", fld, fileName, e);
				throw e;
			}
		}
	}
	
	public InputStream getFileInputStream(String storage, String fld, String filename) {
		if (!Algorithms.isEmpty(storage)) {
			for (String id : storage.split(",")) {
				StorageType st = getStorageProviderById(id);
				if (st != null && !st.local) {
					try {
						S3Object obj = st.s3Conn.getObject(new GetObjectRequest(st.bucket, fld + FILE_SEPARATOR + filename));
						return obj.getObjectContent();
					} catch (com.amazonaws.SdkClientException e) {
						handleException(st, "getFileInputStream", st.bucket, filename, e);
						throw e;
					} catch (RuntimeException e) {
						LOGGER.warn(String.format("Request %s: %s ", st.bucket, fld + FILE_SEPARATOR + filename)); 
						throw e;
					}
				}
			}
		}
		return null;
	}
	
	public void deleteFile(String storage, String fld, String filename) {
		if (!Algorithms.isEmpty(storage)) {
			for (String id : storage.split(",")) {
				StorageType st = getStorageProviderById(id);
				if (st != null && !st.local) {
					try {
						st.s3Conn.deleteObject(st.bucket, fld + FILE_SEPARATOR + filename);
					} catch (com.amazonaws.SdkClientException e) {
						handleException(st, "delete", fld, filename, e);
						throw e;
					}
				}
			}
		}
	}

	private void handleException(StorageType st, String req, String fld, String fileName, SdkClientException e) {
		LOGGER.warn(String.format(
				"StorageError: request %s to file %s - %s has failed (%s)", req, fld, fileName, e.getMessage()));
		if (e.getCause() instanceof org.apache.http.conn.ConnectionPoolTimeoutException) {
			if (System.currentTimeMillis() - reconnectTime > 60 * 1000) {
				st.s3Conn = st.s3ConnBuilder.build();
				reconnectTime = System.currentTimeMillis();
			}
		} else if (e instanceof AmazonS3Exception s3Exception) {
			if ("NoSuchKey".equals(s3Exception.getErrorCode())) {
				LOGGER.error(String.format(
						"The specified key does not exist: %s/%s in bucket %s", fld, fileName, st.bucket));
			} else {
				LOGGER.error(String.format(
						"Amazon S3 error occurred: %s. Request: %s/%s in bucket %s",
						s3Exception.getErrorCode(), fld, fileName, st.bucket));
			}
		} else {
			LOGGER.error(String.format(
					"Unexpected error during request %s to file %s/%s in bucket %s: %s",
					req, fld, fileName, st.bucket, e.getMessage()));
		}
	}

	static class StorageType {
		AmazonS3ClientBuilder s3ConnBuilder;
		AmazonS3 s3Conn;
		String bucket;
		boolean local;
	}

	
	public static class InternalZipFile {
    	
    	
    	private long contentSize;
    	private byte[] data;
		private MultipartFile multipartfile;
		private File tempzipfile;
		
		public long getContentSize() {
			return contentSize;
		}
		
		public byte[] getBytes() throws IOException {
			if (data != null) {
				return data;
			}
			if (multipartfile != null) {
				return multipartfile.getBytes();
			}
			if (tempzipfile != null) {
				return Files.readAllBytes(tempzipfile.toPath());
			}
			throw new IllegalStateException();
		}
		
		public long getSize() {
			if (data != null) {
				return data.length;
			}
			if (multipartfile != null) {
				return multipartfile.getSize();
			}
			if (tempzipfile != null ) {
				return tempzipfile.length();
			}
			throw new IllegalStateException();
		}
		
		public InputStream getInputStream() throws IOException {
			if (data != null) {
				return new ByteArrayInputStream(data);
			}
			if (multipartfile != null) {
				return multipartfile.getInputStream();
			}
			if (tempzipfile != null) {
				return new FileInputStream(tempzipfile);
			}
			throw new IllegalStateException();
        }
		
		public static InternalZipFile buildFromFile(File file) throws IOException {
			InternalZipFile zipfile = new InternalZipFile();
			byte[] buffer = new byte[1024];
			String path = file.getPath();
			zipfile.tempzipfile = new File(path.substring(0, path.lastIndexOf(".gpx")) + GPX_GZ);
			zipfile.contentSize = file.length();
			try (FileInputStream fis = new FileInputStream(file);
			     FileOutputStream fos = new FileOutputStream(zipfile.tempzipfile);
			     GZIPOutputStream gos = new GZIPOutputStream(fos)) {
				int len;
				while ((len = fis.read(buffer)) != -1) {
					gos.write(buffer, 0, len);
				}
			}
			return zipfile;
		}

    	public static InternalZipFile buildFromServerFile(ServerCommonFile file, String name) throws IOException {
    		InternalZipFile zipfile = new InternalZipFile();
			zipfile.contentSize = file.di.getContentSize();
			byte[] bytes = ("{\"type\":\"link\",\"name\":\"" + name + "\"}").getBytes();
            ByteArrayOutputStream bous = new ByteArrayOutputStream();
            GZIPOutputStream gz = new GZIPOutputStream(bous);
            Algorithms.streamCopy(new ByteArrayInputStream(bytes), gz);
            gz.close();
            zipfile.data = bous.toByteArray();
			return zipfile;
    	}
		
    	public static InternalZipFile buildFromMultipartFile(MultipartFile file) throws IOException {
			InternalZipFile zipfile = new InternalZipFile();
			zipfile.contentSize = 0;
			zipfile.multipartfile = file;
			int cnt;
			GZIPInputStream gzis = new GZIPInputStream(file.getInputStream());
			byte[] buf = new byte[1024];
			while ((cnt = gzis.read(buf)) >= 0) {
				zipfile.contentSize += cnt;
			}
			return zipfile;
		}
    }
}
