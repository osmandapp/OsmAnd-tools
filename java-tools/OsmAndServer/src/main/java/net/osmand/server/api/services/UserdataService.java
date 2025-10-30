package net.osmand.server.api.services;

import static org.springframework.http.MediaType.APPLICATION_OCTET_STREAM;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.transaction.Transactional;

import net.osmand.server.api.repo.*;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.io.KFile;
import okio.GzipSource;
import okio.Okio;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.api.services.DownloadIndexesService.ServerCommonFile;
import net.osmand.server.api.services.StorageService.InternalZipFile;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.server.controllers.user.MapApiController;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.server.utils.exception.OsmAndPublicApiException;
import net.osmand.util.Algorithms;

@Service
public class UserdataService {

    @Autowired
    protected UserSubscriptionService userSubService;

    @Autowired
    protected DownloadIndexesService downloadService;

    @Autowired
    protected CloudUserFilesRepository filesRepository;

	@Lazy
	@Autowired
	ShareFileService shareFileService;

    @Autowired
    protected StorageService storageService;

    @Autowired
    @Lazy
    PasswordEncoder encoder;

    @Autowired
    protected CloudUsersRepository usersRepository;

    @Autowired
    EmailSenderService emailSender;

    @Autowired
    protected CloudUserDevicesRepository devicesRepository;

	@Autowired
	protected DeviceSubscriptionsRepository subscriptionsRepo;

	@Autowired
	protected DeviceInAppPurchasesRepository inAppPurchasesRepo;

	@Autowired
	JdbcTemplate jdbcTemplate;

    @Autowired
    WebGpxParser webGpxParser;

    @Autowired
    protected GpxService gpxService;

    Gson gson = new Gson();

    public static final String ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE = "File is not available";
    public static final String BRAND_DEVICE_WEB = "OsmAnd";
    public static final String MODEL_DEVICE_WEB = "Web";
    public static final String TOKEN_DEVICE_WEB = "web";
    public static final int ERROR_CODE_PRO_USERS = 100;
    private static final long MB = 1024 * 1024;
    public static final int BUFFER_SIZE = 1024 * 512;
    public static final long MAXIMUM_ACCOUNT_SIZE = 3000 * MB; // 3 (5 GB - std, 50 GB - ext, 1000 GB - pro)
    private static final String USER_FOLDER_PREFIX = "user-";
    private static final String FILE_NAME_SUFFIX = ".gz";
    private static final String CR_SANITIZE = "$0D"; // \r
    private static final String LF_SANITIZE = "$0A"; // \n

    private static final int ERROR_CODE_EMAIL_IS_INVALID = 1 + ERROR_CODE_PRO_USERS;
    private static final int ERROR_CODE_NO_VALID_SUBSCRIPTION = 2 + ERROR_CODE_PRO_USERS;
    public static final int ERROR_CODE_USER_IS_NOT_REGISTERED = 3 + ERROR_CODE_PRO_USERS;
    private static final int ERROR_CODE_TOKEN_IS_NOT_VALID_OR_EXPIRED = 4 + ERROR_CODE_PRO_USERS;
    public static final int ERROR_CODE_PROVIDED_TOKEN_IS_NOT_VALID = 5 + ERROR_CODE_PRO_USERS;
    public static final int ERROR_CODE_FILE_NOT_AVAILABLE = 6 + ERROR_CODE_PRO_USERS;
    public static final int ERROR_CODE_GZIP_ONLY_SUPPORTED_UPLOAD = 7 + ERROR_CODE_PRO_USERS;
    private static final int ERROR_CODE_SIZE_OF_SUPPORTED_BOX_IS_EXCEEDED = 8 + ERROR_CODE_PRO_USERS;
//    private static final int ERROR_CODE_SUBSCRIPTION_WAS_EXPIRED_OR_NOT_PRESENT = 10 + ERROR_CODE_PRO_USERS;
    private static final int ERROR_CODE_PASSWORD_IS_TO_SIMPLE = 12 + ERROR_CODE_PRO_USERS;

    private static final int MAX_NUMBER_OF_FILES_FREE_ACCOUNT = 10000;
    public static final long MAXIMUM_FREE_ACCOUNT_SIZE = 5 * MB;
    private static final long MAXIMUM_FREE_ACCOUNT_FILE_SIZE = 1 * MB;
    public static final String FILE_TYPE_GLOBAL = "GLOBAL";
    public static final String FILE_TYPE_FAVOURITES = "FAVOURITES";
    public static final String FILE_TYPE_PROFILE = "PROFILE";
    public static final String FILE_TYPE_GPX = "GPX";
    public static final String FILE_TYPE_OSM_EDITS = "OSM_EDITS";
    public static final String FILE_TYPE_OSM_NOTES = "OSM_NOTES";
    public static final Set<String> FREE_TYPES = Set.of(FILE_TYPE_FAVOURITES, FILE_TYPE_GLOBAL, FILE_TYPE_PROFILE, FILE_TYPE_OSM_EDITS, FILE_TYPE_OSM_NOTES);
    public static final String EMPTY_FILE_NAME = "__folder__.info";
    public static final String INFO_EXT = ".info";

	public static final String FILE_NOT_FOUND = "File not found";
	public static final String FILE_WAS_DELETED = "File was deleted";

    protected static final Log LOG = LogFactory.getLog(UserdataService.class);
    
    private static final int MAX_ATTEMPTS_PER_DAY = 100;
    private final Cache<String, RequestData> requestTracker = CacheBuilder.newBuilder()
            .expireAfterWrite(24, TimeUnit.HOURS)
            .build();
    
    private static class RequestData {
        public int checkCount;
        public long lastCheckTime;
        
        public RequestData(int checkCount, long lastCheckTime) {
            this.checkCount = checkCount;
            this.lastCheckTime = lastCheckTime;
        }
    }
    
    private ResponseEntity<String> trackRequest(HttpServletRequest request) {
        String ipAddress = request.getRemoteAddr();
        RequestData checkData = requestTracker.getIfPresent(ipAddress);
        
        if (checkData == null) {
            checkData = new RequestData(0, System.currentTimeMillis());
        }
        if (checkData.checkCount >= MAX_ATTEMPTS_PER_DAY) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body("Too many requests. Try again later.");
        }
        checkData.checkCount++;
        checkData.lastCheckTime = System.currentTimeMillis();
        requestTracker.put(ipAddress, checkData);
        
        return null;
    }

    public void validateUserForUpload(CloudUserDevicesRepository.CloudUserDevice dev, String type, long fileSize) {
    	CloudUser user = usersRepository.findById(dev.userid);
        if (user == null) {
            throw new OsmAndPublicApiException(ERROR_CODE_USER_IS_NOT_REGISTERED, "Unexpected error: user is not registered.");
        }

        String errorMsg = userSubService.verifyAndRefreshProOrderId(user);

		if (errorMsg != null || Algorithms.isEmpty(user.orderid)) {
			if (!FREE_TYPES.contains(type)) {
				throw new OsmAndPublicApiException(ERROR_CODE_NO_VALID_SUBSCRIPTION,
						String.format("Free account can upload files with types: %s. This file type is %s!", ArrayUtils.toString(FREE_TYPES) , type));
			}
			if (fileSize > MAXIMUM_FREE_ACCOUNT_FILE_SIZE) {
                throw new OsmAndPublicApiException(ERROR_CODE_SIZE_OF_SUPPORTED_BOX_IS_EXCEEDED, String.format("File size exceeded, %d > %d!", fileSize / MB, MAXIMUM_FREE_ACCOUNT_FILE_SIZE / MB));
            }
		}

        UserdataController.UserFilesResults res = generateFiles(user.id, null, false, false, Collections.emptySet());
        if (res.totalZipSize > MAXIMUM_ACCOUNT_SIZE) {
            throw new OsmAndPublicApiException(ERROR_CODE_SIZE_OF_SUPPORTED_BOX_IS_EXCEEDED,
                    "Maximum size of OsmAnd Cloud exceeded " + (MAXIMUM_ACCOUNT_SIZE / MB)
                            + " MB. Please contact support in order to investigate possible solutions.");
        }

		if (Algorithms.isEmpty(user.orderid)) {
			if (res.totalFiles > MAX_NUMBER_OF_FILES_FREE_ACCOUNT) {
				throw new OsmAndPublicApiException(ERROR_CODE_SIZE_OF_SUPPORTED_BOX_IS_EXCEEDED,
						"Maximum size of OsmAnd Cloud exceeded " + (MAXIMUM_ACCOUNT_SIZE / MB)
								+ " MB. Please contact support in order to investigate possible solutions.");
			}
		}
        if (errorMsg != null || Algorithms.isEmpty(user.orderid)) {
            UserdataController.UserFilesResults files = generateFiles(user.id, null, false, false, FREE_TYPES);
            if (files.totalZipSize + fileSize > MAXIMUM_FREE_ACCOUNT_SIZE) {
                throw new OsmAndPublicApiException(ERROR_CODE_SIZE_OF_SUPPORTED_BOX_IS_EXCEEDED, String.format("Not enough space to save file. Maximum size of OsmAnd Cloud for Free account %d!", MAXIMUM_FREE_ACCOUNT_FILE_SIZE / MB));
            }
        }
    }

	public UserdataController.UserFilesResults generateFiles(int userId, String name, boolean allVersions, boolean details, Set<String> types) {
		List<CloudUserFilesRepository.UserFileNoData> allFiles = new ArrayList<>();
		List<UserFileNoData> fl;
		if (types != null && !types.isEmpty()) {
			for (String t : types) {
				fl = details ? filesRepository.listFilesByUseridWithDetails(userId, name, t) :
						filesRepository.listFilesByUserid(userId, name, t);
				allFiles.addAll(fl);
				if (t == null) {
					break;
				}
			}
		} else {
			fl = details ? filesRepository.listFilesByUseridWithDetails(userId, name, null) :
					filesRepository.listFilesByUserid(userId, name, null);
			allFiles.addAll(fl);
		}

		sanitizeFileNames(allFiles);

		return getUserFilesResults(allFiles, userId, allVersions);
	}

	public Set<String> parseFileTypes(String types) {
		if (types == null || types.isEmpty()) {
			return Collections.emptySet();
		}

		Set<String> result = new LinkedHashSet<>();
		for (String s : types.split(",")) {
			if (!s.isBlank()) {
				result.add(s.trim());
			}
		}
		return result;
	}

	private void sanitizeFileNames(List<UserFileNoData> files) {
		for(UserFileNoData f : files) {
			f.name = sanitizeEncode(f.name);
		}
	}

	public String sanitizeEncode(String name) {
		return name.replace("\r", CR_SANITIZE).replace("\n", LF_SANITIZE);
	}

	private String sanitizeDecode(String name) {
		return name.replace(CR_SANITIZE, "\r").replace(LF_SANITIZE, "\n");
	}

	public UserdataController.UserFilesResults getUserFilesResults(List<UserFileNoData> files, int userId, boolean allVersions) {
		files.sort(Comparator
				.comparingLong((UserFileNoData f) -> f.updatetimems)
				.reversed()
		);
        CloudUser user = usersRepository.findById(userId);
        UserdataController.UserFilesResults res = new UserdataController.UserFilesResults();
        res.maximumAccountSize = Algorithms.isEmpty(user.orderid) ? MAXIMUM_FREE_ACCOUNT_SIZE : MAXIMUM_ACCOUNT_SIZE;
        res.uniqueFiles = new ArrayList<>();
        if (allVersions) {
            res.allFiles = new ArrayList<>();
        }
        res.userid = userId;
        Set<String> fileIds = new TreeSet<>();
        for (UserFileNoData sf : files) {
            String fileId = sf.type + "____" + sf.name;
	        boolean isNewestFile = fileIds.add(fileId);

	        if (sf.filesize >= 0) {
		        res.totalFileVersions++;
		        if (allVersions || isNewestFile) {
			        res.totalZipSize += sf.zipSize;
			        res.totalFileSize += sf.filesize;
		        }
	        }

	        if (isNewestFile && sf.filesize >= 0) {
		        res.totalFiles++;
		        res.uniqueFiles.add(sf);
	        }

	        if (allVersions) {
		        res.allFiles.add(sf);
	        }
        }
        return res;
    }

    public ServerCommonFile checkThatObfFileisOnServer(String name, String type) throws IOException {
        boolean checkExistingServerMap = type.equalsIgnoreCase("file") && (
                name.endsWith(".obf") || name.endsWith(".sqlitedb") || name.endsWith(".tif"));
        if (checkExistingServerMap) {
            return downloadService.getServerGlobalFile(name);
        }
        return null;
    }

    public static class ResponseFileStatus {
    	protected long filesize;
    	protected long zipfilesize;
    	protected long updatetime;
    	protected long clienttime;
    	protected String type;
    	protected String name;
    	protected JsonObject details;
    	protected String status = "ok";

    	public ResponseFileStatus(UserFile f) {
    		filesize = f.filesize;
    		zipfilesize = f.zipfilesize;
    		updatetime = f.updatetime == null ? 0 : f.updatetime.getTime();
    		clienttime = f.clienttime == null ? 0 : f.clienttime.getTime();
    		name = f.name;
    		type = f.type;
    		details = f.details;
    	}

        public void setJsonObject(JsonObject details) {
            this.details = details;
        }
    }

    public ResponseEntity<String> uploadMultipartFile(MultipartFile file, CloudUserDevicesRepository.CloudUserDevice dev,
			String name, String type, Long clienttime) throws IOException {
		ServerCommonFile serverCommonFile = checkThatObfFileisOnServer(name, type);
		InternalZipFile zipfile;
		if (serverCommonFile != null) {
			zipfile = InternalZipFile.buildFromServerFile(serverCommonFile, name);
		} else {
			try {
				String originalFilename = file.getOriginalFilename();
				if (originalFilename == null) {
					originalFilename = name;
				}
				if (isKmlKmzFileByName(originalFilename)) {
					InputStream is = new GZIPInputStream(file.getInputStream());
					GpxFile gpxFile = gpxService.importGpx(Okio.source(is), originalFilename);
					File tmpGpxFile = gpxService.createTmpFileByGpxFile(gpxFile, name);
					zipfile = InternalZipFile.buildFromFile(tmpGpxFile);
				} else {
					zipfile = InternalZipFile.buildFromMultipartFile(file);
				}
			} catch (IOException e) {
                throw new OsmAndPublicApiException(ERROR_CODE_GZIP_ONLY_SUPPORTED_UPLOAD, "File is submitted not in gzip format");
			}
		}
		validateUserForUpload(dev, type, zipfile.getSize());
		return uploadFile(zipfile, dev, name, type, clienttime);
	}

	private boolean isKmlKmzFileByName(String originalFilename) {
		return originalFilename.toLowerCase().endsWith(".kml") || originalFilename.toLowerCase().endsWith(".kmz");
	}

	public ResponseEntity<String> uploadFile(InternalZipFile zipfile, CloudUserDevicesRepository.CloudUserDevice dev,
			String name, String type, Long clienttime) throws IOException {
		CloudUserFilesRepository.UserFile usf = new CloudUserFilesRepository.UserFile();
		usf.name = name;
		usf.type = type;
		usf.updatetime = new Date();
		if (clienttime != null) {
			usf.clienttime = new Date(clienttime);
		}
		usf.userid = dev.userid;
		usf.deviceid = dev.id;
		usf.filesize = zipfile.getContentSize();
		usf.zipfilesize = zipfile.getSize();
		usf.storage = storageService.save(userFolder(usf), storageFileName(usf), zipfile);
		if (storageService.storeLocally()) {
			usf.data = zipfile.getBytes();
		}
		filesRepository.saveAndFlush(usf);
		return ResponseEntity.ok(gson.toJson(new ResponseFileStatus(usf)));
	}


    public String userFolder(UserFile uf) {
        return userFolder(uf.userid);
    }

    public String userFolder(int userid) {
        return USER_FOLDER_PREFIX + userid;
    }

    public String storageFileName(UserFile uf) {
        return storageFileName(uf.type, uf.name, uf.updatetime);
    }

    public String storageFileName(String type, String name, Date updatetime) {
        String fldName = type;
        if (name.indexOf('/') != -1) {
            int nt = name.lastIndexOf('/');
            fldName += "/" + name.substring(0, nt);
            name = name.substring(nt + 1);
        }
        return fldName + "/" + updatetime.getTime() + "-" + name + FILE_NAME_SUFFIX;
    }

    public ResponseEntity<String> webUserActivate(String email, String token, String password, String lang) {
        if (password.length() < 8) {
            throw new OsmAndPublicApiException(ERROR_CODE_PASSWORD_IS_TO_SIMPLE, "enter password with at least 8 symbols");
        }
        return registerNewDevice(email, token, TOKEN_DEVICE_WEB, encoder.encode(password), lang, BRAND_DEVICE_WEB, MODEL_DEVICE_WEB);
    }

	public ResponseEntity<String> webUserRegister(String email, String lang, boolean isNew, HttpServletRequest request) {
        ResponseEntity<String> response = trackRequest(request);
        if (response != null) {
            return response;
        }
        
		email = email.toLowerCase().trim();
		if (!emailSender.isEmail(email)) {
            return ResponseEntity.badRequest().body("Email is not valid.");
		}
        CloudUsersRepository.CloudUser pu = usersRepository.findByEmailIgnoreCase(email);
        if (isNew) {
            if (pu != null) {
                List<CloudUserDevicesRepository.CloudUserDevice> devices = devicesRepository.findByUserid(pu.id);
                if (devices != null && !devices.isEmpty()) {
	                userSubService.verifyAndRefreshProOrderId(pu);
                    return ResponseEntity.badRequest().body("An account is already registered with this email address.");
                }
            } else {
                pu = new CloudUsersRepository.CloudUser();
                pu.email = email;
                pu.regTime = new Date();
            }
        }
		if (pu != null) {
            pu.tokendevice = TOKEN_DEVICE_WEB;
            if (pu.token == null || pu.token.length() < UserdataController.SPECIAL_PERMANENT_TOKEN) {
                pu.token = (new Random().nextInt(8999) + 1000) + "";
            }
            pu.tokenTime = new Date();
            usersRepository.saveAndFlush(pu);
            emailSender.sendOsmAndCloudWebEmail(pu.email, pu.token, "@ACTION_SETUP@", lang);
		} else {
            return ResponseEntity.badRequest().body("error_email");
        }
  
		return ok();
	}
    
    public ResponseEntity<String> validateToken(String email, String token) {
        CloudUsersRepository.CloudUser pu = usersRepository.findByEmailIgnoreCase(email);
        if (pu == null) {
            return ResponseEntity.badRequest().body("error_email");
        }
        if (pu.token == null || !pu.token.equals(token) || pu.tokenTime == null || System.currentTimeMillis()
                - pu.tokenTime.getTime() > TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS)) {
            wearOutToken(pu);
            return ResponseEntity.badRequest().body("error_token");
        }
        return ok();
    }
    
    public ResponseEntity<String> checkUserEmail(String email) {
        CloudUsersRepository.CloudUser pu = usersRepository.findByEmailIgnoreCase(email);
        if (pu == null) {
            return ResponseEntity.badRequest().body("error_email");
        }
        return ok();
    }

    public ResponseEntity<String> ok() {
        return ResponseEntity.ok(gson.toJson(Collections.singletonMap("status", "ok")));
    }

    public String hideEmail(String email) {
        if (email == null) {
            return "***";
        }
        int at = email.indexOf("@");
        if (at == -1) {
            return email;
        }
        String name = email.substring(0, at);
        StringBuilder hdName = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (i > 0 && i < name.length() - 1) {
                c = '*';
            }
            hdName.append(c);
        }
        return hdName.toString() + email.substring(at);
    }

    public ResponseEntity<String> registerNewDevice(String email, String token, String deviceId, String accessToken,
                                                    String lang, String brand, String model) {
		if (Algorithms.isEmpty(email)) {
			LOG.error("device-register: email is empty (" + email + ")");
			throw new OsmAndPublicApiException(ERROR_CODE_USER_IS_NOT_REGISTERED, "empty email");
		}
        email = email.toLowerCase().trim();
        CloudUsersRepository.CloudUser pu = usersRepository.findByEmailIgnoreCase(email);
        if (pu == null) {
            LOG.error("device-register: email is not found (" + email + ")");
            throw new OsmAndPublicApiException(ERROR_CODE_USER_IS_NOT_REGISTERED, "user with that email is not registered");
        }
        boolean tokenIsActive = pu.token != null && pu.tokenTime != null &&
                (System.currentTimeMillis() - pu.tokenTime.getTime()) <
                        TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
        if ( ! (tokenIsActive && pu.token.equals(token))) {
            wearOutToken(pu); // cut down on tries (even for web password)
            if ( ! (tokenIsActive && validateWithWebPassword(pu.id, token))) {
                LOG.error("device-register: invalid token (" + email + ") [" + token + "]");
                throw new OsmAndPublicApiException(ERROR_CODE_TOKEN_IS_NOT_VALID_OR_EXPIRED, "token is not valid or expired (24h)");
            }
        }
        if (pu.token.length() < UserdataController.SPECIAL_PERMANENT_TOKEN) {
        	pu.token = null;
        }
        pu.tokenTime = null;
        CloudUserDevicesRepository.CloudUserDevice device = new CloudUserDevicesRepository.CloudUserDevice();
	    if (Algorithms.isEmpty(deviceId) || Algorithms.isEmpty(model)) {
		    LOG.error("device-register: avoid delete-anonymous-same-device (" + email + ")");
	    } else {
		    CloudUserDevicesRepository.CloudUserDevice sameDevice;
		    while ((sameDevice = devicesRepository
				    .findTopByUseridAndDeviceidAndModelOrderByUdpatetimeDesc(pu.id, deviceId, model)) != null) {
				LOG.error(String.format(
						"device-register: delete-same-device (%s) id (%d) brand (%s) model (%s) deviceId (%s)",
						email, sameDevice.id, brand, model, deviceId));
			    devicesRepository.delete(sameDevice);
		    }
	    }
        device.lang = lang;
        device.brand = brand;
        device.model = model;
        device.userid = pu.id;
        device.deviceid = deviceId;
        device.udpatetime = new Date();
        device.accesstoken = accessToken;
        usersRepository.saveAndFlush(pu);

	    userSubService.verifyAndRefreshProOrderId(pu);

        devicesRepository.saveAndFlush(device);
        LOG.info("device-register: success (" + email + ")");
        return ResponseEntity.ok(gson.toJson(device));
    }

    private boolean validateWithWebPassword(int userId, String password) {
        if (userId > 0 && !Algorithms.isEmpty(password)) {
            List<CloudUserDevicesRepository.CloudUserDevice> webDevices =
                    devicesRepository.findByUseridAndDeviceid(userId, TOKEN_DEVICE_WEB);
            for (CloudUserDevicesRepository.CloudUserDevice device : webDevices) {
                if (!Algorithms.isEmpty(device.accesstoken) && encoder.matches(password, device.accesstoken)) {
                    return true;
                }
            }
        }
        return false;
    }

    public String oldStorageFileName(CloudUserFilesRepository.UserFile usf) {
        String fldName = usf.type;
        String name = usf.name;
        return fldName + "/" + usf.updatetime.getTime() + "-" + name + FILE_NAME_SUFFIX;
    }

    public void getFile(CloudUserFilesRepository.UserFile userFile, HttpServletResponse response, HttpServletRequest request, String name, String type,
                        CloudUserDevicesRepository.CloudUserDevice dev, Boolean simplified) throws IOException {
        InputStream bin = null;
        File fileToDelete = null;
        try {
			ServerCommonFile scf = checkThatObfFileisOnServer(name, type);
			boolean gzin = true, gzout;
			if (scf != null) {
				// file is not stored here
				File fp = scf.file;
				if (scf.url != null) {
					String bname = name;
					if (bname.lastIndexOf('/') != -1) {
						bname = bname.substring(bname.lastIndexOf('/') + 1);
					}
					fp = File.createTempFile("backup_", bname);
					fileToDelete = fp;
					FileOutputStream fous = new FileOutputStream(fp);
					InputStream is = scf.url.openStream();
					Algorithms.streamCopy(is, fous);
					fous.close();
				}
				if (fp != null) {
					gzin = false;
					bin = getGzipInputStreamFromFile(fp, ".obf");
				}
			} else {
				bin = getInputStream(dev, userFile);
			}

			// Simplify GPX file
			if (simplified != null && simplified && "GPX".equalsIgnoreCase(type) && bin != null) {
				InputStream sourceStream = gzin ? new GZIPInputStream(bin) : bin;
				GpxFile gpxFile = GpxUtilities.INSTANCE.loadGpxFile(Okio.source(sourceStream));
				if (gpxFile.getError() == null) {
					GpxFile simplifiedGpx = gpxService.createSimplifiedGpxFile(gpxFile);
					File tmpGpx = File.createTempFile("simplified_", ".gpx");
					Exception exception = GpxUtilities.INSTANCE.writeGpxFile(new KFile(tmpGpx.getAbsolutePath()), simplifiedGpx);
					if (exception == null) {
						if (fileToDelete != null) {
							try {
								Files.delete(fileToDelete.toPath());
							} catch (IOException e) {
								LOG.warn("Could not delete temp file: " + fileToDelete.getAbsolutePath(), e);
							}
						}
						fileToDelete = tmpGpx;
						bin.close();
						bin = new FileInputStream(tmpGpx);
						gzin = false;
					}
				}
			}

            String safeDownloadName = URLEncoder.encode(sanitizeEncode(userFile.name));
            response.setHeader("Content-Disposition", "attachment; filename=" + safeDownloadName);
            // InputStream bin = fl.data.getBinaryStream();

            String acceptEncoding = request.getHeader("Accept-Encoding");
            if (acceptEncoding != null && acceptEncoding.contains("gzip")) {
                response.setHeader("Content-Encoding", "gzip");
                gzout = true;
            } else {
                if (gzin) {
                    bin = new GZIPInputStream(bin);
                }
                gzout = false;
            }
            response.setContentType(APPLICATION_OCTET_STREAM.getType());
            byte[] buf = new byte[BUFFER_SIZE];
            int r;
            OutputStream ous = gzout && !gzin ? new GZIPOutputStream(response.getOutputStream()) : response.getOutputStream();
			if (bin != null) {
				while ((r = bin.read(buf)) != -1) {
					ous.write(buf, 0, r);
				}
			}
			ous.close();
		} finally {
			if (bin != null) {
				bin.close();
			}
			if (fileToDelete != null) {
				fileToDelete.delete();
			}
		}
    }
    
    public ResponseEntity<String> restoreFile(String name, String type, Long updatetime, CloudUserDevicesRepository.CloudUserDevice dev) throws IOException {
        CloudUserFilesRepository.UserFile userFile = getUserFile(name, type, updatetime, dev);
        if (userFile == null) {
            return ResponseEntity.badRequest().body("File not found");
        }
        if (userFile.zipfilesize >= 0) {
            return ResponseEntity.badRequest().body("File is not deleted");
        }
        if (checkIfRestoredVersionExists(name, type, updatetime, dev)) {
            return ResponseEntity.badRequest().body("File has already been restored from this version");
        }
        UserFile prevFile = getFilePrevVersion(name, type, userFile.updatetime.getTime(), dev);
        if (prevFile == null || prevFile.zipfilesize <= 0) {
            return ResponseEntity.badRequest().body("Previous version of file not found");
        }
        CloudUserFilesRepository.UserFile usf = new CloudUserFilesRepository.UserFile();
        InternalZipFile zipFile = getZipFile(prevFile, prevFile.name);
        if (zipFile == null) {
            return ResponseEntity.badRequest().body("Error restore file");
        }
        try {
            validateUserForUpload(dev, type, zipFile.getSize());
        } catch (OsmAndPublicApiException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
        usf.name = prevFile.name;
        usf.type = type;
        usf.updatetime = new Date();
        usf.clienttime = prevFile.clienttime;
        usf.userid = dev.userid;
        usf.deviceid = dev.id;
        usf.filesize = zipFile.getContentSize();
        usf.zipfilesize = zipFile.getSize();
        usf.storage = storageService.save(userFolder(usf), storageFileName(usf), zipFile);
        if (storageService.storeLocally()) {
            usf.data = zipFile.getBytes();
        }
        filesRepository.saveAndFlush(usf);
        return ResponseEntity.ok(gson.toJson(new UserFileNoData(usf)));
    }
    
    public boolean checkIfRestoredVersionExists(String name, String type, Long updatetime, CloudUserDevicesRepository.CloudUserDevice dev) {
        UserFile file = filesRepository.findTopByUseridAndNameAndTypeAndUpdatetimeGreaterThanOrderByUpdatetimeDesc(dev.userid, name, type, new Date(updatetime));
        return file != null;
    }
    
    @Transactional
    public ResponseEntity<String> emptyTrash(List<MapApiController.FileData> files, CloudUserDevicesRepository.CloudUserDevice dev) {
        for (MapApiController.FileData file : files) {
            deleteFileAllVersions(dev.userid, file.name, file.type, file.updatetime, true);
        }
        return ok();
    }

    private InputStream getGzipInputStreamFromFile(File fp, String ext) throws IOException {
        if (fp.getName().endsWith(".zip")) {
            ZipInputStream zis = new ZipInputStream(new FileInputStream(fp));
            ZipEntry ze = zis.getNextEntry();
            while (ze != null && !ze.getName().endsWith(ext)) {
                ze = zis.getNextEntry();
            }
            if (ze != null) {
                return zis;
            }
        }
        return new FileInputStream(fp);
    }


    public InputStream getInputStream(CloudUserDevicesRepository.CloudUserDevice dev, CloudUserFilesRepository.UserFile userFile) {
        InputStream bin = null;
        if (dev == null) {
	        tokenNotValidError();
        } else {
            if (userFile == null) {
                throw new OsmAndPublicApiException(ERROR_CODE_FILE_NOT_AVAILABLE, ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
            } else if (userFile.data == null) {
                bin = getInputStream(userFile);
                if (bin == null) {
                    throw new OsmAndPublicApiException(ERROR_CODE_FILE_NOT_AVAILABLE, ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
                }
            } else {
                bin = new ByteArrayInputStream(userFile.data);
            }
        }
        return bin;
    }

    public CloudUserFilesRepository.UserFile getUserFile(String name, String type, Long updatetime, CloudUserDevicesRepository.CloudUserDevice dev) {
        if (dev == null) {
            return null;
        }
        name = sanitizeDecode(name);
        if (updatetime != null) {
            return filesRepository.findTopByUseridAndNameAndTypeAndUpdatetime(dev.userid, name, type,
                    new Date(updatetime));
        } else {
            return filesRepository.findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(dev.userid, name, type);
        }
    }

    public CloudUserFilesRepository.UserFile getFilePrevVersion(String name, String type, Long updatetime, CloudUserDevicesRepository.CloudUserDevice dev) {
        if (dev == null) {
            return null;
        }
        return filesRepository.findTopByUseridAndNameAndTypeAndUpdatetimeLessThanOrderByUpdatetimeDesc(dev.userid, name, type,
                new Date(updatetime));
    }
    
    public InputStream getInputStream(CloudUserFilesRepository.UserFile userFile) {
		if (!Algorithms.isEmpty(userFile.storage) && userFile.storage.equals("local")) {
			return new ByteArrayInputStream(userFile.data);
		}
        return storageService.getFileInputStream(userFile.storage, userFolder(userFile), storageFileName(userFile));
    }

    public InputStream getInputStream(CloudUserFilesRepository.UserFileNoData userFile) {
        return storageService.getFileInputStream(userFile.storage, userFolder(userFile.userid), storageFileName(userFile.type, userFile.name, userFile.updatetime));
    }

    public ResponseEntity<String> tokenNotValidError() {
        throw new OsmAndPublicApiException(ERROR_CODE_PROVIDED_TOKEN_IS_NOT_VALID, "provided deviceid or token is not valid");
    }

	public ResponseEntity<String> tokenNotValidResponse() {
		return new ResponseEntity<>("Unauthorized", HttpStatus.UNAUTHORIZED);
	}

	@Transactional
    public void deleteFile(String name, String type, Integer deviceId, Long clienttime, CloudUserDevicesRepository.CloudUserDevice dev) {
        CloudUserFilesRepository.UserFile usf = new CloudUserFilesRepository.UserFile();
        usf.name = name;
        usf.type = type;
        usf.updatetime = new Date();
        usf.userid = dev.userid;
        usf.deviceid = deviceId != null ? deviceId : dev.id;
        usf.data = null;
        usf.filesize = -1L;
        usf.zipfilesize = -1L;
        if (clienttime != null) {
            usf.clienttime = new Date(clienttime);
        }
        filesRepository.saveAndFlush(usf);
		shareFileService.deleteShareFile(name, dev.userid);
    }

    //delete entry from database!
    public ResponseEntity<String> deleteFileVersion(Long updatetime, int userid, String fileName, String fileType, UserFile file) {
        UserFile userFile = file;
        if (updatetime != null && userFile == null) {
            userFile = filesRepository.findTopByUseridAndNameAndTypeAndUpdatetime(userid, fileName, fileType,
                    new Date(updatetime));
        }
        if (userFile == null) {
            throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE, ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
        }
        storageService.deleteFile(userFile.storage, userFolder(userFile), storageFileName(userFile));
        filesRepository.delete(userFile);
        return ok();
    }
    
    @Transactional
    public ResponseEntity<String> deleteFileAllVersions(int userid, String fileName, String fileType, Long updatetime, boolean isTrash) {
        List<UserFile> files = filesRepository.findAllByUseridAndNameAndTypeOrderByUpdatetimeDesc(userid, fileName, fileType);
        if (files.isEmpty()) {
            return ResponseEntity.badRequest().body("File not found");
        }
        if (isTrash && files.get(0).zipfilesize > 0) {
            return ResponseEntity.badRequest().body("This is not trash, the file is not deleted");
        }
        if (files.get(0).updatetime.getTime() != updatetime) {
            return ResponseEntity.badRequest().body("File version was changed");
        }
        for (UserFile file : files) {
            storageService.deleteFile(file.storage, userFolder(file), storageFileName(file));
            filesRepository.delete(file);
        }
        return ok();
    }

    @Transactional
    public ResponseEntity<String> renameFile(String oldName, String newName, String type, CloudUserDevicesRepository.CloudUserDevice dev, boolean saveCopy) throws IOException {
        CloudUserFilesRepository.UserFile file = getLastFileVersion(dev.userid, oldName, type);
        if (file != null) {
            InternalZipFile zipFile = getZipFile(file, newName);
            if (zipFile != null) {
                try {
                    validateUserForUpload(dev, type, zipFile.getSize());
                } catch (OsmAndPublicApiException e) {
                    return ResponseEntity.badRequest().body(e.getMessage());
                }
                //create file with new name
                ResponseEntity<String> res = uploadFile(zipFile, dev, newName, type, System.currentTimeMillis());
                if (res.getStatusCode().is2xxSuccessful()) {
                    if (!saveCopy) {
                        //delete file with old name
                        deleteFile(oldName, type, null, null, dev);
                    }
                    return ok();
                }
            } else {
                // skip deleted files
                return ok();
            }
        }
        return ResponseEntity.badRequest().body(saveCopy ? "Error create duplicate file!" : "Error rename file!");
    }

    public InternalZipFile getZipFile(CloudUserFilesRepository.UserFile file, String newName) throws IOException {
        InternalZipFile zipFile = null;
        File tmpGpx = File.createTempFile(newName, ".gpx");
        if (file.filesize == 0 && file.name.endsWith(EMPTY_FILE_NAME)) {
            zipFile = InternalZipFile.buildFromFile(tmpGpx);
        } else {
            InputStream in = file.data != null ? new ByteArrayInputStream(file.data) : getInputStream(file);
            if (in != null) {
	            GpxFile gpxFile = GpxUtilities.INSTANCE.loadGpxFile(null, new GzipSource(Okio.source(in)), null, false);
				if (gpxFile.getError() != null) {
					return null;
				}
                Exception exception = GpxUtilities.INSTANCE.writeGpxFile(new KFile(tmpGpx.getAbsolutePath()), gpxFile);
                if (exception != null) {
                    return null;
                }
                zipFile = InternalZipFile.buildFromFile(tmpGpx);
            }
        }
        return zipFile;
    }

    @Transactional
    public ResponseEntity<String> renameFolder(String folderName, String newFolderName, String type, CloudUserDevicesRepository.CloudUserDevice dev) throws IOException {
        Iterable<UserFile> files = filesRepository.findLatestFilesByFolderName(dev.userid, folderName + "/", type);
        for (UserFile file : files) {
            String newName = file.name.replaceFirst(Pattern.quote(folderName), newFolderName);
            ResponseEntity<String> response = renameFile(file.name, newName, type, dev, false);
            if (!response.getStatusCode().is2xxSuccessful()) {
                return response;
            }
        }
        return ok();
    }

    @Transactional
    public ResponseEntity<String> deleteFolder(String folderName, String type, CloudUserDevicesRepository.CloudUserDevice dev) {
        Iterable<UserFile> files = filesRepository.findLatestFilesByFolderName(dev.userid, folderName + "/", type);
        for (UserFile file : files) {
            if (file.filesize != -1) {
                deleteFile(file.name, type, null, null, dev);
            }
        }
        return ok();
    }

    public CloudUsersRepository.CloudUser getUserById(int id) {
        return usersRepository.findById(id);
    }

    public UserFile getLastFileVersion(int id, String fileName, String fileType) {
        return filesRepository.findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(id, fileName, fileType);
    }

	public void updateFileSize(UserFileNoData ufnd) {
		Optional<UserFile> op = filesRepository.findById(ufnd.id);
		if (op.isEmpty()) {
			LOG.error("Couldn't find user file by id: " + ufnd.id);
			return;
		}
		UserFile uf = op.get();
		if (uf.data != null && uf.data.length > 10000) {
			throw new UnsupportedOperationException();
		}
		uf.zipfilesize = ufnd.zipSize;
		uf.filesize = ufnd.filesize;
		filesRepository.saveAndFlush(uf);
		uf = filesRepository.getById(ufnd.id);
	}


    private boolean isSelectedType(Set<String> filterTypes, CloudUserFilesRepository.UserFileNoData sf) {
        final String FILE_TYPE = "FILE";
        final String FILE_TYPE_MAPS = "FILE_MAPS";
        final String FILE_TYPE_OTHER = "FILE_OTHER";
        String currentFileType = sf.type.toUpperCase();
        if (filterTypes.contains(currentFileType)) {
            return true;
        }
        if (currentFileType.equals(FILE_TYPE)) {
            List<String> fileTypes = filterTypes.stream()
                    .filter(type -> type.startsWith(FILE_TYPE))
                    .toList();
            if (!fileTypes.isEmpty()) {
                String subtypeFolder = FileSubtype.getSubtypeByFileName(sf.name).getSubtypeFolder();
                if (subtypeFolder != null) {
                    String currentFileSubType = subtypeFolder.replace("/", "");
                    if (!currentFileSubType.equals("")) {
                        return fileTypes.stream().anyMatch(type -> currentFileSubType.equalsIgnoreCase(StringUtils.substringAfter(type, FILE_TYPE + "_")));
                    } else {
                        return fileTypes.contains(FILE_TYPE_MAPS) || fileTypes.contains(FILE_TYPE_OTHER);
                    }
                }
            }
        }
        return false;
    }

    public String toJson(String type, String name) throws JSONException {
        JSONObject json = new JSONObject();
        name = addName(json, type, name);
        json.put("type", type);
        json.put("subtype", FileSubtype.getSubtypeByFileName(name).getSubtypeName());

        return json.toString();
    }

    private String addName(JSONObject json, String type, String name) {
        if (type.equalsIgnoreCase(FILE_TYPE_GPX)) {
            name = "tracks" + File.separatorChar + name;
        }
        json.put("file", name);
        return name;
    }

    protected JSONObject createItemsJson(JSONArray itemsJson) throws JSONException {
        final int VERSION = 1;

        JSONObject json = new JSONObject();
        json.put("version", VERSION);
        json.put("items", itemsJson);

        return json;
    }


    public void getBackup(HttpServletResponse response, CloudUserDevicesRepository.CloudUserDevice dev,
			Set<String> filterTypes, boolean includeDeleted, String format) throws IOException {
		List<UserFileNoData> files = filesRepository.listFilesByUserid(dev.userid, null, null);
		Set<String> fileIds = new TreeSet<>();
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yy");
		String fileName = "Export_" + formatter.format(new Date());
		File tmpFile = File.createTempFile(fileName, ".zip");
		response.setHeader("Content-Disposition", "attachment; filename=" + fileName + format);
		response.setHeader("Content-Type", "application/zip");
		ZipOutputStream zs = null;
		try {
            JSONArray itemsJson = new JSONArray();
			zs = new ZipOutputStream(new FileOutputStream(tmpFile));
			for (CloudUserFilesRepository.UserFileNoData sf : files) {
				String fileId = sf.type + "____" + sf.name;
                if (shouldSkipFile(filterTypes, sf, null)) {
                    continue;
                }
				if (fileIds.add(fileId)) {
					if (sf.filesize >= 0) {
                        itemsJson.put(new JSONObject(toJson(sf.type, sf.name)));
                        InputStream s3is = getInputStream(sf);
                        InputStream is;
                        if (s3is == null) {
                            CloudUserFilesRepository.UserFile userFile = getUserFile(sf.name, sf.type, null, dev);
                            if (userFile != null) {
                                is = new GZIPInputStream(getInputStream(dev, userFile));
                            } else {
                                is = null;
                            }
                        } else {
                            is = new GZIPInputStream(s3is);
                        }
                        ZipEntry zipEntry;
                        if (format.equals(".zip")) {
                            zipEntry = new ZipEntry(sf.type + File.separatorChar + sf.name);
                        } else {
                            if (sf.type.equalsIgnoreCase(FILE_TYPE_GPX)) {
                                zipEntry = new ZipEntry("tracks" + File.separatorChar + sf.name);
                            } else {
                                zipEntry = new ZipEntry(sf.name);
                            }
                        }
						zs.putNextEntry(zipEntry);
                        if (is != null) {
                            Algorithms.streamCopy(is, zs);
                        }
						zs.closeEntry();
					} else if (includeDeleted) {
						// include last version of deleted files
						fileIds.remove(fileId);
					}
				}
			}
            JSONObject json = createItemsJson(itemsJson);
            ZipEntry zipEntry = new ZipEntry("items.json");
            zs.putNextEntry(zipEntry);
            InputStream is = new ByteArrayInputStream(json.toString().getBytes());
            Algorithms.streamCopy(is, zs);
            zs.closeEntry();
			zs.flush();
			zs.finish();
			zs.close();
			response.setHeader("Content-Length", tmpFile.length() + "");
			FileInputStream fis = new FileInputStream(tmpFile);
			try {
				OutputStream ous = response.getOutputStream();
				Algorithms.streamCopy(fis, ous);
				ous.close();
			} finally {
				fis.close();
			}
		} finally {
			if (zs != null) {
				zs.close();
			}
			tmpFile.delete();
		}
	}

    private boolean shouldSkipFile(Set<String> filterTypes, UserFileNoData userFileNoData, UserFile userFile) {
        if (userFileNoData != null) {
            // get backup for all files
            return (filterTypes != null && !isSelectedType(filterTypes, userFileNoData))
                    || userFileNoData.name.endsWith(EMPTY_FILE_NAME)
                    || userFileNoData.name.endsWith(INFO_EXT);
        } else if (userFile != null) {
            // get backup for folder
            return (filterTypes != null && !filterTypes.contains(userFile.type))
                    || userFile.filesize == -1
                    || userFile.name.endsWith(EMPTY_FILE_NAME)
                    || userFile.name.endsWith(INFO_EXT);
        }
        return false;
    }

    @Transactional
    public void getBackupFolder(HttpServletResponse response, CloudUserDevicesRepository.CloudUserDevice dev,
                                String folderName, String format, String type, List<CloudUserFilesRepository.UserFile> selectedFiles) throws IOException {
        List<UserFile> files = folderName != null
		        ? filesRepository.findLatestFilesByFolderName(dev.userid, folderName + "/", type)
		        : selectedFiles;
        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yy");
        String fileName = "Export_" + formatter.format(new Date());
        File tmpFile = File.createTempFile(fileName, ".zip");
        try (ZipOutputStream zs = new ZipOutputStream(new FileOutputStream(tmpFile))) {
            JSONArray itemsJson = new JSONArray();
            for (UserFile file : files) {
                if (!shouldSkipFile(Collections.singleton(type), null, file)) {
                    itemsJson.put(new JSONObject(toJson(type, file.name)));
                    InputStream is = new GZIPInputStream(getInputStream(dev, file));
                    ZipEntry zipEntry = new ZipEntry(file.name);
                    zs.putNextEntry(zipEntry);
                    Algorithms.streamCopy(is, zs);
                    zs.closeEntry();
                }
            }
            JSONObject json = createItemsJson(itemsJson);
            ZipEntry zipEntry = new ZipEntry("items.json");
            zs.putNextEntry(zipEntry);
            InputStream is = new ByteArrayInputStream(json.toString().getBytes());
            Algorithms.streamCopy(is, zs);
            zs.closeEntry();
            zs.flush();
            zs.finish();
            response.setHeader("Content-Disposition", "attachment; filename=" + fileName + format);
            response.setHeader("Content-Type", "application/zip");
            response.setHeader("Content-Length", tmpFile.length() + "");
            try (FileInputStream fis = new FileInputStream(tmpFile)) {
                OutputStream ous = response.getOutputStream();
                Algorithms.streamCopy(fis, ous);
                ous.close();
            }
        } finally {
            Files.delete(tmpFile.toPath());
        }
    }


    @Transactional
    public ResponseEntity<String> deleteAccount(String token, CloudUserDevicesRepository.CloudUserDevice dev, HttpServletRequest request) throws ServletException {
        CloudUsersRepository.CloudUser pu = usersRepository.findById(dev.userid);
        if (pu != null && pu.id == dev.userid) {
            boolean tokenExpired = System.currentTimeMillis() - pu.tokenTime.getTime() > TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
            boolean validToken = pu.token.equals(token) && !tokenExpired;
            wearOutToken(pu);
            if (validToken) {
                if (deleteAllFiles(dev)) {
                    int numOfUsersDelete = usersRepository.deleteByEmailIgnoreCase(pu.email);
                    if (numOfUsersDelete != -1) {
						LOG.info("Deleted (/delete-account) users with email " + pu.email + " and id " + pu.id);
						removeUserIdFromPurchases(pu.id);
                        int numOfUserDevicesDelete = devicesRepository.deleteByUserid(dev.userid);
                        if (numOfUserDevicesDelete != -1) {
							LOG.info("Deleted (/delete-account) user devices for user " + pu.email + " and id " + pu.id);
                            request.logout();
                            return ResponseEntity.ok(String.format("Account has been successfully deleted (email %s)", pu.email));
                        }
                    }
                    return ResponseEntity.badRequest().body(String.format("Account hasn't been deleted (%s)", pu.email));
                } else {
                    return ResponseEntity.badRequest().body(String.format("Unable to delete user files (%s)", pu.email));
                }
            }
            return ResponseEntity.badRequest().body("Token is not valid or expired (24h), or password is not valid");
        }
        return ResponseEntity.badRequest().body("Email doesn't match login username");
    }

	private void removeUserIdFromPurchases(int userId) {
		List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions = subscriptionsRepo.findAllByUserId(userId);
		if (subscriptions != null && !subscriptions.isEmpty()) {
			for (DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription : subscriptions) {
				subscription.userId = null;
				subscriptionsRepo.save(subscription);
			}
			LOG.info("Removed userid from subscriptions for user with id " + userId);
		}
		List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> inAppPurchases = inAppPurchasesRepo.findByUserId(userId);
		if (inAppPurchases != null && !inAppPurchases.isEmpty()) {
			for (DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase inAppPurchase : inAppPurchases) {
				inAppPurchase.userId = null;
				inAppPurchasesRepo.save(inAppPurchase);
			}
			LOG.info("Removed userid from in-app purchases for user with id " + userId);
		}
	}

    private boolean deleteAllFiles(CloudUserDevicesRepository.CloudUserDevice dev) {
        Iterable<UserFile> files = filesRepository.findAllByUserid(dev.userid);
        files.forEach(file -> {
            storageService.deleteFile(file.storage, userFolder(file), storageFileName(file));
            filesRepository.delete(file);
        });

        files = filesRepository.findAllByUserid(dev.userid);
        return IterableUtils.size(files) == 0;
    }

    @Transactional
    public ResponseEntity<String> sendCode(String action, String lang, CloudUsersRepository.CloudUser pu) {
        if (!("setup".equals(action) || "change".equals(action) || "delete".equals(action))) {
            return ok();
        }
        if (pu == null) {
            return ResponseEntity.badRequest().body("Email is not registered");
        }
        String token = (new Random().nextInt(8999) + 1000) + "";
        emailSender.sendOsmAndCloudWebEmail(pu.email, token, action, lang);
        pu.token = token;
        pu.tokenTime = new Date();
        usersRepository.saveAndFlush(pu);

	    userSubService.verifyAndRefreshProOrderId(pu);

        return ok();
    }

    public ResponseEntity<String> confirmCode(String code, CloudUserDevicesRepository.CloudUserDevice dev) {
        CloudUsersRepository.CloudUser pu = usersRepository.findById(dev.userid);
        if (pu == null) {
            return ResponseEntity.badRequest().body("User is not registered");
        }
        boolean tokenExpired = System.currentTimeMillis() - pu.tokenTime.getTime() > TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
        wearOutToken(pu);
        if (pu.token.equals(code) && !tokenExpired) {
            return ok();
        } else {
            return ResponseEntity.badRequest().body("Token is not valid or expired (24h)");
        }
    }

    public ResponseEntity<String> confirmCode(String username, String token) {
        if (token == null) {
            return ResponseEntity.badRequest().body("Token is not valid");
        }
        CloudUsersRepository.CloudUser pu = usersRepository.findByEmailIgnoreCase(username);
        if (pu == null) {
            return ResponseEntity.badRequest().body("User is not registered");
        }
        boolean tokenExpired = System.currentTimeMillis() - pu.tokenTime.getTime() > TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
        wearOutToken(pu);
        if (pu.token.equals(token) && !tokenExpired) {
            return ok();
        } else {
            return ResponseEntity.badRequest().body("Token is not valid or expired (24h)");
        }
    }

    public void wearOutToken(CloudUsersRepository.CloudUser pu) {
        if (pu == null || pu.tokenTime == null) {
            return;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(pu.tokenTime);
        cal.add(Calendar.HOUR_OF_DAY, -7);
        pu.tokenTime = cal.getTime();

        usersRepository.saveAndFlush(pu);
    }


    public ResponseEntity<String> changeEmail(String username, String token, CloudUserDevicesRepository.CloudUserDevice dev, HttpServletRequest request) throws ServletException {
        // validate new email
        CloudUsersRepository.CloudUser tempUser = usersRepository.findByEmailIgnoreCase(username);
        if (tempUser == null) {
            return ResponseEntity.badRequest().body("Something went wrong with your new email");
        }
        if (tempUser.orderid != null) {
            String errorMsg = userSubService.checkOrderIdPro(tempUser.orderid);
            if (errorMsg != null) {
                return ResponseEntity.badRequest().body("You can't change email, because you have subscription on new email");
            }
        } else {
            List<CloudUserFilesRepository.UserFileNoData> allFiles = filesRepository.listFilesByUserid(tempUser.id, null, null);
            if (!allFiles.isEmpty()) {
                return ResponseEntity.badRequest().body("You can't change email, because you have files in account on new email");
            }
        }
        List<CloudUserDevicesRepository.CloudUserDevice> devices = devicesRepository.findByUserid(tempUser.id);
        if (!devices.isEmpty()) {
            return ResponseEntity.badRequest().body("You can't change email, because you have devices in account on new email");
        }
        // validate token
        ResponseEntity<String> response = confirmCode(username, token);
        if (!response.getStatusCode().is2xxSuccessful()) {
            return response;
        }
        // validate current user
        CloudUsersRepository.CloudUser currentUser = usersRepository.findById(dev.userid);
        if (currentUser == null) {
            return ResponseEntity.badRequest().body("User is not registered");
        }
        // change email
        usersRepository.delete(tempUser);
        currentUser.email = username;
        usersRepository.saveAndFlush(currentUser);
	    LOG.info("User email changed (/auth/change-email) from " + tempUser.email + " to " + currentUser.email);

	    userSubService.verifyAndRefreshProOrderId(currentUser);

        request.logout();

        return ok();
    }

    public void updateDeviceLangInfo(CloudUserDevicesRepository.CloudUserDevice dev, String lang, String brand, String model) {
        if (dev != null) {
            dev.lang = (lang == null) ? dev.lang : lang;
            dev.brand = (brand == null) ? dev.brand : brand;
            dev.model = (model == null) ? dev.model : model;
            devicesRepository.saveAndFlush(dev);
        }
    }

	public Map<String, GpxFile> getGpxFilesMap(CloudUserDevicesRepository.CloudUserDevice dev,
	                                           List<String> names, List<UserFile> selectedFiles) throws IOException {
		Map<String, GpxFile> files = new HashMap<>();
		if (names != null && !names.isEmpty()) {
			for (String name : names) {
				if (!name.endsWith(EMPTY_FILE_NAME)) {
					UserFile userFile = getUserFile(name, "GPX", null, dev);
					if (userFile != null) {
						processGpxFile(dev, userFile, files);
					}
				}
			}
		} else if (selectedFiles != null) {
			for (UserFile userFile : selectedFiles) {
				if (!userFile.name.endsWith(EMPTY_FILE_NAME)) {
					processGpxFile(dev, userFile, files);
				}
			}
		}
		return files;
	}

	public UserdataController.UserFilesResults generateGpxFilesByQuadTiles(int userId, boolean allVersions, String[] tiles) {
		String query = "SELECT u.id, u.userid, u.deviceid, u.type, u.name, u.updatetime, u.clienttime, u.filesize, u.zipfilesize, u.storage " +
				"FROM user_files u " +
				"WHERE u.userid = ? " +
				"AND u.type = 'GPX' " +
				"AND u.shortlinktiles && CAST(? AS text[]) " +
				"ORDER BY u.updatetime DESC";


		List<Map<String, Object>> rows = jdbcTemplate.queryForList(query, userId, tiles);

		List<UserFileNoData> userFileNoDataList = new ArrayList<>();
		for (Map<String, Object> row : rows) {
			long id = (Long) row.get("id");
			int uId = (Integer) row.get("userid");
			int deviceId = (Integer) row.get("deviceid");
			String type = (String) row.get("type");
			String name = (String) row.get("name");
			Date updateTime = (Date) row.get("updatetime");
			Date clientTime = (Date) row.get("clienttime");
			Long fileSize = (Long) row.get("filesize");
			Long zipFileSize = (Long) row.get("zipfilesize");
			String storage = (String) row.get("storage");

			userFileNoDataList.add(new UserFileNoData(id, uId, deviceId, type, name, updateTime, clientTime, fileSize, zipFileSize, storage, null));
		}

		sanitizeFileNames(userFileNoDataList);

		return getUserFilesResults(userFileNoDataList, userId, allVersions);
	}

	private void processGpxFile(CloudUserDevicesRepository.CloudUserDevice dev, UserFile userFile,
	                            Map<String, GpxFile> files) throws IOException {
		try (InputStream is = getInputStream(dev, userFile)) {
			GpxFile file = GpxUtilities.INSTANCE.loadGpxFile(null, new GzipSource(Okio.source(is)), null, false);
			if (file.getError() == null) {
				file.setModifiedTime(userFile.updatetime.getTime());
				files.put(userFile.name, file);
			}
		}
	}
}
