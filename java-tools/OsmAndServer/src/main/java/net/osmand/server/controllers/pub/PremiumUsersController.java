package net.osmand.server.controllers.pub;

import static org.springframework.http.MediaType.APPLICATION_OCTET_STREAM;
import static org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;

import net.osmand.server.api.repo.PremiumUserDeviceRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFile;
import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.PremiumUserRepository;

@RestController
@RequestMapping("/userdata")
public class PremiumUsersController {

	private static final int ERROR_CODE_PREMIUM_USERS = 100;
	private static final int ERROR_CODE_FILE_NOT_AVAILABLE = 4 + ERROR_CODE_PREMIUM_USERS;
	private static final int ERROR_CODE_GZIP_ONLY_SUPPORTED_UPLOAD = 5 + ERROR_CODE_PREMIUM_USERS;

	protected static final Log LOG = LogFactory.getLog(PremiumUsersController.class);

	Gson gson = new Gson();
	
    @Autowired
    protected PremiumUserRepository usersRepository;
    
    @Autowired
    protected PremiumUserFilesRepository filesRepository;
    
    @Autowired
    protected PremiumUserDeviceRepository deviceRepository;
    
//    @PersistenceContext 
//    protected EntityManager entityManager;
    

    
    protected ResponseEntity<String> error(int errorCode, String message) {
    	Map<String, Object> mp = new TreeMap<String, Object>();
    	mp.put("errorCode", errorCode);
    	mp.put("message", message);
    	return ResponseEntity.badRequest().body(gson.toJson(Collections.singletonMap("error", mp)));
    }
    
    
    private ResponseEntity<String> ok() {
    	return ResponseEntity.ok(gson.toJson(Collections.singletonMap("status", "ok")));
    }
    
    
    
	
	@PostMapping(value = "/delete")
	@ResponseBody
	public ResponseEntity<String> delete(@RequestParam(name = "name", required = true) String name, @RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken)
			throws IOException {
		UserFile usf = new PremiumUserFilesRepository.UserFile();
		usf.name = name;
		usf.type = type;
		usf.updatetime = new Date();
		usf.userid = deviceId;
		usf.deviceid = deviceId;
		usf.data = null;
		usf.filesize = -1;
		filesRepository.saveAndFlush(usf);
		return ok();
	}
	
	@PostMapping(value = "/upload", consumes = MULTIPART_FORM_DATA_VALUE)
	@ResponseBody
	public ResponseEntity<String> upload(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file,
			@RequestParam(name = "name", required = true) String name, @RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken)
			throws IOException {
		UserFile usf = new PremiumUserFilesRepository.UserFile();
		int cnt, sum;
		try {
			GZIPInputStream gzis = new GZIPInputStream(file.getInputStream());
			byte[] buf = new byte[1024];
			sum = 0;
			while((cnt =gzis.read(buf)) >= 0 ) {
				sum += cnt;
			}
		} catch (IOException e) {
			return error(ERROR_CODE_GZIP_ONLY_SUPPORTED_UPLOAD, "File is submitted not in gzip format");
		}
		usf.name = name;
		usf.type = type;
		usf.updatetime = new Date();
		// TODO proper userid
		usf.userid = deviceId;
		usf.deviceid = deviceId;
		usf.filesize = sum;
//		Session session = entityManager.unwrap(Session.class);
//	    Blob blob = session.getLobHelper().createBlob(file.getInputStream(), file.getSize());
//		usf.data = blob;
		 
		usf.data = file.getBytes();
		filesRepository.saveAndFlush(usf);
		return ok();
	}
	

	
	@GetMapping(value = "/download")
	@ResponseBody
	public void getFile(HttpServletResponse response, HttpServletRequest request,
			@RequestParam(name = "name", required = true) String name, @RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "deviceid", required = true) int deviceId,
			@RequestParam(name = "accessToken", required = true) String accessToken) throws IOException, SQLException {
		UserFile fl = filesRepository.findTopByDeviceidAndNameAndTypeOrderByUpdatetimeDesc(deviceId, name, type);
		if (fl.data == null) {
			ResponseEntity<String> entity = error(ERROR_CODE_FILE_NOT_AVAILABLE, "File is not available");
			response.setStatus(entity.getStatusCodeValue());
			response.getWriter().write(entity.getBody());
			return;
		}
		response.setHeader("Content-Disposition", "attachment; filename=" + fl.name);
		// InputStream bin = fl.data.getBinaryStream();
		InputStream bin = new ByteArrayInputStream(fl.data);
		String acceptEncoding = request.getHeader("Accept-Encoding");
		if (acceptEncoding != null && acceptEncoding.contains("gzip")) {
			response.setHeader("Content-Encoding", "gzip");
		} else {
			bin = new GZIPInputStream(bin);
		}
		response.setContentType(APPLICATION_OCTET_STREAM.getType());
		byte[] buf = new byte[1024];
		int r;
		while ((r = bin.read(buf)) != -1) {
			response.getOutputStream().write(buf, 0, r);
		}
	}

	
	@GetMapping(value = "/list")
	@ResponseBody
	public ResponseEntity<String> listFiles( 
			@RequestParam(name = "deviceid", required = true) int deviceId, @RequestParam(name = "accessToken", required = true) String accessToken, 
			@RequestParam(name = "name", required = false) String name, @RequestParam(name = "type", required = false) String type,
			@RequestParam(name = "allVersions", required = false, defaultValue = "false") boolean allVersions) throws IOException, SQLException {
		// TODO retrieve userid
		List<UserFileNoData> fl = filesRepository.listFilesByUserid(deviceId, name, type);
		UserFilesResults res = new UserFilesResults();
		res.uniqueFiles = new ArrayList<>();
		if (allVersions) {
			res.allFiles = new ArrayList<>();
		}
		res.deviceid = deviceId;
		Set<String> fileIds = new TreeSet<String>();
		for (UserFileNoData sf : fl) {
			String fileId = sf.type + "____" + sf.name;
			if (fileIds.add(fileId)) {
				if (sf.filesize >= 0) {
					res.uniqueFiles.add(sf);
				}
			}
			if (allVersions) {
				res.allFiles.add(sf);
			}
		}
		return ResponseEntity.ok(gson.toJson(res));
	}
	

	public static class UserFilesResults {
		public List<UserFileNoData> allFiles;
		public List<UserFileNoData> uniqueFiles;
		public int deviceid;
		
	}
}