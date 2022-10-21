package net.osmand.server.api.services;

import static org.springframework.http.MediaType.APPLICATION_OCTET_STREAM;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;

import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.api.repo.PremiumUsersRepository;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.util.Algorithms;

@Service
public class UserdataService {
    
    @Autowired
    protected UserSubscriptionService userSubService;
    
    @Autowired
    protected DownloadIndexesService downloadService;
    
    @Autowired
    protected PremiumUserFilesRepository filesRepository;
    
    @Autowired
    protected StorageService storageService;
    
    @Autowired
    @Lazy
    PasswordEncoder encoder;
    
    @Autowired
    protected PremiumUsersRepository usersRepository;
    
    @Autowired
    EmailSenderService emailSender;
    
    @Autowired
    protected PremiumUserDevicesRepository devicesRepository;
    
    Gson gson = new Gson();
    
    public static final String TOKEN_DEVICE_WEB = "web";
    public static final int ERROR_CODE_PREMIUM_USERS = 100;
    public static final int ERROR_CODE_GZIP_ONLY_SUPPORTED_UPLOAD = 7 + ERROR_CODE_PREMIUM_USERS;
    public static final int ERROR_CODE_USER_IS_NOT_REGISTERED = 3 + ERROR_CODE_PREMIUM_USERS;
    private static final int ERROR_CODE_SUBSCRIPTION_WAS_EXPIRED_OR_NOT_PRESENT = 10 + ERROR_CODE_PREMIUM_USERS;
    private static final long MB = 1024 * 1024;
    private static final long MAXIMUM_ACCOUNT_SIZE = 3000 * MB; // 3 (5 GB - std, 50 GB - ext, 1000 GB - premium)
    private static final int ERROR_CODE_SIZE_OF_SUPPORTED_BOX_IS_EXCEEDED = 8 + ERROR_CODE_PREMIUM_USERS;
    private static final String USER_FOLDER_PREFIX = "user-";
    private static final String FILE_NAME_SUFFIX = ".gz";
    private static final int ERROR_CODE_TOKEN_IS_NOT_VALID_OR_EXPIRED = 4 + ERROR_CODE_PREMIUM_USERS;
    private static final int ERROR_CODE_PROVIDED_TOKEN_IS_NOT_VALID = 5 + ERROR_CODE_PREMIUM_USERS;
    //    private static final int ERROR_CODE_GZIP_ONLY_SUPPORTED_UPLOAD = 7 + ERROR_CODE_PREMIUM_USERS;
    private static final int ERROR_CODE_PASSWORD_IS_TO_SIMPLE = 12 + ERROR_CODE_PREMIUM_USERS;
    private static final int BUFFER_SIZE = 1024 * 512;
    private static final int ERROR_CODE_EMAIL_IS_INVALID = 1 + ERROR_CODE_PREMIUM_USERS;
    private static final int ERROR_CODE_NO_VALID_SUBSCRIPTION = 2 + ERROR_CODE_PREMIUM_USERS;
    
    public static final int ERROR_CODE_FILE_NOT_AVAILABLE = 6 + ERROR_CODE_PREMIUM_USERS;
    
    protected static final Log LOG = LogFactory.getLog(UserdataController.class);
    
    public ResponseEntity<String> validateUser(PremiumUsersRepository.PremiumUser user) {
        if (user == null) {
            return error(ERROR_CODE_USER_IS_NOT_REGISTERED, "Unexpected error: user is not registered.");
        }
        String errorMsg = userSubService.checkOrderIdPremium(user.orderid);
        if (errorMsg != null) {
            return error(ERROR_CODE_SUBSCRIPTION_WAS_EXPIRED_OR_NOT_PRESENT,
                    "Subscription is not valid any more: " + errorMsg);
        }
        UserdataController.UserFilesResults res = generateFiles(user.id, null, null, false, false);
        if (res.totalZipSize > MAXIMUM_ACCOUNT_SIZE) {
            return error(ERROR_CODE_SIZE_OF_SUPPORTED_BOX_IS_EXCEEDED,
                    "Maximum size of OsmAnd Cloud exceeded " + (MAXIMUM_ACCOUNT_SIZE / MB)
                            + " MB. Please contact support in order to investigate possible solutions.");
        }
        return null;
    }
    
    public UserdataController.UserFilesResults generateFiles(int userId, String name, String type, boolean allVersions, boolean details) {
        List<PremiumUserFilesRepository.UserFileNoData> fl =
                details ? filesRepository.listFilesByUseridWithDetails(userId, name, type) :
                        filesRepository.listFilesByUserid(userId, name, type);
        UserdataController.UserFilesResults res = new UserdataController.UserFilesResults();
        res.maximumAccountSize = MAXIMUM_ACCOUNT_SIZE;
        res.uniqueFiles = new ArrayList<>();
        if (allVersions) {
            res.allFiles = new ArrayList<>();
        }
        res.userid = userId;
        Set<String> fileIds = new TreeSet<String>();
        for (PremiumUserFilesRepository.UserFileNoData sf : fl) {
            String fileId = sf.type + "____" + sf.name;
            if (sf.filesize >= 0) {
                res.totalFileVersions++;
                res.totalZipSize += sf.zipSize;
                res.totalFileSize += sf.filesize;
            }
            if (fileIds.add(fileId)) {
                if (sf.filesize >= 0) {
                    res.totalFiles++;
                    res.uniqueFiles.add(sf);
                }
            }
            if (allVersions) {
                res.allFiles.add(sf);
                
            }
        }
        return res;
    }
    
    public boolean checkThatObfFileisOnServer(String name, String type) throws IOException {
        boolean checkExistingServerMap = type.toLowerCase().equals("file") && (
                name.endsWith(".obf") || name.endsWith(".sqlitedb"));
        if (checkExistingServerMap) {
            String fp = downloadService.getFilePathUrl(name);
            if (fp == null) {
                LOG.info("File is not found: " + name);
                checkExistingServerMap = false;
            }
        }
        return checkExistingServerMap;
    }
    
    public MultipartFile createEmptyMultipartFile(MultipartFile file) {
        return new MultipartFile() {
            
            @Override
            public void transferTo(File dest) throws IOException, IllegalStateException {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public boolean isEmpty() {
                return getSize() == 0;
            }
            
            @Override
            public long getSize() {
                try {
                    return getBytes().length;
                } catch (IOException e) {
                    return 0;
                }
            }
            
            @Override
            public String getOriginalFilename() {
                return file.getOriginalFilename();
            }
            
            @Override
            public String getName() {
                return file.getName();
            }
            
            @Override
            public InputStream getInputStream() throws IOException {
                return new ByteArrayInputStream(getBytes());
            }
            
            @Override
            public String getContentType() {
                return file.getContentType();
            }
            
            @Override
            public byte[] getBytes() throws IOException {
                byte[] bytes = "{\"type\":\"obf\"}".getBytes();
                ByteArrayOutputStream bous = new ByteArrayOutputStream();
                GZIPOutputStream gz = new GZIPOutputStream(bous);
                Algorithms.streamCopy(new ByteArrayInputStream(bytes), gz);
                gz.close();
                return bous.toByteArray();
            }
        };
    }
    
    public ResponseEntity<String> error(int errorCode, String message) {
        Map<String, Object> mp = new TreeMap<>();
        mp.put("errorCode", errorCode);
        mp.put("message", message);
        return ResponseEntity.badRequest().body(gson.toJson(Collections.singletonMap("error", mp)));
    }
    
    public ResponseEntity<String> uploadFile(MultipartFile file, PremiumUserDevicesRepository.PremiumUserDevice dev,
                                             String name, String type, Long clienttime) throws IOException {
        PremiumUserFilesRepository.UserFile usf = new PremiumUserFilesRepository.UserFile();
        long cnt;
        long sum;
        boolean checkExistingServerMap = checkThatObfFileisOnServer(name, type);
        if (checkExistingServerMap) {
            file = createEmptyMultipartFile(file);
        }
        long zipsize = file.getSize();
        try {
            GZIPInputStream gzis = new GZIPInputStream(file.getInputStream());
            byte[] buf = new byte[1024];
            sum = 0;
            while ((cnt = gzis.read(buf)) >= 0) {
                sum += cnt;
            }
        } catch (IOException e) {
            return error(ERROR_CODE_GZIP_ONLY_SUPPORTED_UPLOAD, "File is submitted not in gzip format");
        }
        usf.name = name;
        usf.type = type;
        usf.updatetime = new Date();
		if (clienttime != null) {
			usf.clienttime = new Date(clienttime);
		}
        usf.userid = dev.userid;
        usf.deviceid = dev.id;
        usf.filesize = sum;
        usf.zipfilesize = zipsize;
        usf.storage = storageService.save(userFolder(usf), storageFileName(usf), file);
        if (storageService.storeLocally()) {
            usf.data = file.getBytes();
        }
        filesRepository.saveAndFlush(usf);
        return null;
    }
    
    public String userFolder(PremiumUserFilesRepository.UserFile usf) {
        return USER_FOLDER_PREFIX + usf.userid;
    }
    
    public String storageFileName(PremiumUserFilesRepository.UserFile usf) {
        String fldName = usf.type;
        String name = usf.name;
        if (name.indexOf('/') != -1) {
            int nt = name.lastIndexOf('/');
            fldName += "/" + name.substring(0, nt);
            name = name.substring(nt + 1);
        }
        return fldName + "/" + usf.updatetime.getTime() + "-" + name + FILE_NAME_SUFFIX;
    }
    
    public ResponseEntity<String> webUserActivate(String email, String token, String password) throws IOException {
        if (password.length() < 6) {
            return error(ERROR_CODE_PASSWORD_IS_TO_SIMPLE, "enter password with at least 6 symbols");
        }
        return registerNewDevice(email, token, TOKEN_DEVICE_WEB, encoder.encode(password));
    }
    
    public ResponseEntity<String> webUserRegister(@RequestParam(name = "email", required = true) String email) throws IOException {
        // allow to register only with small case
        email = email.toLowerCase().trim();
        if (!email.contains("@")) {
            return error(ERROR_CODE_EMAIL_IS_INVALID, "email is not valid to be registered");
        }
        PremiumUsersRepository.PremiumUser pu = usersRepository.findByEmail(email);
        if (pu == null) {
            return error(ERROR_CODE_EMAIL_IS_INVALID, "email is not registered");
        }
        String errorMsg = userSubService.checkOrderIdPremium(pu.orderid);
        if (errorMsg != null) {
            return error(ERROR_CODE_NO_VALID_SUBSCRIPTION, errorMsg);
        }
        pu.tokendevice = TOKEN_DEVICE_WEB;
        pu.token = (new Random().nextInt(8999) + 1000) + "";
        pu.tokenTime = new Date();
        usersRepository.saveAndFlush(pu);
        emailSender.sendOsmAndCloudWebEmail(pu.email, pu.token);
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
    
    public ResponseEntity<String> registerNewDevice(String email, String token, String deviceId, String accessToken) {
        email = email.toLowerCase().trim();
        PremiumUsersRepository.PremiumUser pu = usersRepository.findByEmail(email);
        if (pu == null) {
            return error(ERROR_CODE_USER_IS_NOT_REGISTERED, "user with that email is not registered");
        }
        if (pu.token == null || !pu.token.equals(token) || pu.tokenTime == null || System.currentTimeMillis()
                - pu.tokenTime.getTime() > TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS)) {
            return error(ERROR_CODE_TOKEN_IS_NOT_VALID_OR_EXPIRED, "token is not valid or expired (24h)");
        }
        pu.token = null;
        pu.tokenTime = null;
        PremiumUserDevicesRepository.PremiumUserDevice device = new PremiumUserDevicesRepository.PremiumUserDevice();
        PremiumUserDevicesRepository.PremiumUserDevice sameDevice;
        while ((sameDevice = devicesRepository.findTopByUseridAndDeviceidOrderByUdpatetimeDesc(pu.id,
                deviceId)) != null) {
            devicesRepository.delete(sameDevice);
        }
        device.userid = pu.id;
        device.deviceid = deviceId;
        device.udpatetime = new Date();
        device.accesstoken = accessToken;
        usersRepository.saveAndFlush(pu);
        devicesRepository.saveAndFlush(device);
        return ResponseEntity.ok(gson.toJson(device));
    }
    
    public String oldStorageFileName(PremiumUserFilesRepository.UserFile usf) {
        String fldName = usf.type;
        String name = usf.name;
        return fldName + "/" + usf.updatetime.getTime() + "-" + name + FILE_NAME_SUFFIX;
    }
    
    public void getFile(HttpServletResponse response, HttpServletRequest request, String name, String type,
                        Long updatetime, PremiumUserDevicesRepository.PremiumUserDevice dev) throws IOException {
        InputStream bin = null;
        File fileToDelete = null;
        try {
            @SuppressWarnings("unchecked")
            ResponseEntity<String>[] error = new ResponseEntity[]{null};
			PremiumUserFilesRepository.UserFile userFile = getUserFile(name, type, updatetime, dev);
			String existingServerMapUrl = type.toLowerCase().equals("file") ? downloadService.getFilePathUrl(name)
					: null;
			boolean gzin = true, gzout;
			if (existingServerMapUrl != null) {
				// file is not stored here
				File fp = new File(existingServerMapUrl);
				if (existingServerMapUrl.startsWith("https://") || existingServerMapUrl.startsWith("http://")) {
					String bname = name;
					if (bname.lastIndexOf('/') != -1) {
						bname = bname.substring(bname.lastIndexOf('/') + 1);
					}
					fp = File.createTempFile("backup_", bname);
					fileToDelete = fp;
					FileOutputStream fous = new FileOutputStream(fp);
					URL url = new URL(existingServerMapUrl);
					InputStream is = url.openStream();
					Algorithms.streamCopy(is, fous);
					fous.close();
				}
				if (fp != null) {
					gzin = false;
					bin = getGzipInputStreamFromFile(fp, ".obf");
				}
			} else {
				bin = getInputStream(dev, error, userFile);
			}
            if (error[0] != null) {
                response.setStatus(error[0].getStatusCodeValue());
                response.getWriter().write(error[0].getBody());
                return;
            }
            response.setHeader("Content-Disposition", "attachment; filename=" + userFile.name);
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
            while ((r = bin.read(buf)) != -1) {
                ous.write(buf, 0, r);
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
    
    
    public InputStream getInputStream(PremiumUserDevicesRepository.PremiumUserDevice dev, ResponseEntity<String>[] error, PremiumUserFilesRepository.UserFile userFile) {
        InputStream bin = null;
        if (dev == null) {
            error[0] = tokenNotValid();
        } else {
            if (userFile == null) {
                error[0] = error(ERROR_CODE_FILE_NOT_AVAILABLE, "File is not available");
            } else if (userFile.data == null) {
                bin = getInputStream(userFile);
                if (bin == null) {
                    error[0] = error(ERROR_CODE_FILE_NOT_AVAILABLE, "File is not available");
                }
            } else {
                bin = new ByteArrayInputStream(userFile.data);
            }
        }
        return bin;
    }
    
    public PremiumUserFilesRepository.UserFile getUserFile(String name, String type, Long updatetime, PremiumUserDevicesRepository.PremiumUserDevice dev) {
        if (dev == null) {
            return null;
        }
        if (updatetime != null) {
            return filesRepository.findTopByUseridAndNameAndTypeAndUpdatetime(dev.userid, name, type,
                    new Date(updatetime));
        } else {
            return filesRepository.findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(dev.userid, name, type);
        }
    }
    
    public InputStream getInputStream(PremiumUserFilesRepository.UserFile userFile) {
        return storageService.getFileInputStream(userFile.storage, userFolder(userFile), storageFileName(userFile));
    }
    
    public ResponseEntity<String> tokenNotValid() {
        return error(ERROR_CODE_PROVIDED_TOKEN_IS_NOT_VALID, "provided deviceid or token is not valid");
    }
    
    public void deleteFile(String name, String type, Integer deviceId, Long clienttime, PremiumUserDevicesRepository.PremiumUserDevice dev) {
        PremiumUserFilesRepository.UserFile usf = new PremiumUserFilesRepository.UserFile();
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
    }
}
