package net.osmand.server.controllers.user;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import net.osmand.obf.diff.ObfFileInMemory;
import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.services.FavoriteService;
import net.osmand.server.api.services.GpxService;
import net.osmand.server.api.services.StorageService.InternalZipFile;
import net.osmand.server.controllers.user.AdminController.AdminGenericSubReport;
import net.osmand.server.controllers.user.AdminController.Subscription;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.api.services.MotdService.MotdSettings;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.server.utils.exception.OsmAndPublicApiException;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.WptPt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

import static net.osmand.router.RouteExporter.OSMAND_ROUTER_V2;
import static net.osmand.shared.gpx.GpxUtilities.HIDDEN_EXTENSION;

@Controller
@RequestMapping("/admin/issues")
public class IssuesController {
	

	@Value("${osmand.web.location}")
	private String websiteLocation;

	@Value("${osmand.files.location}")
	private String filesLocation;
	
	private static final String ISSUES_FOLDER = "servers/github_issues/issues";
	
	@GetMapping(path = {"/download-issues"})
	public ResponseEntity<FileSystemResource> releaseDownload(String file) {
		File fl = new File(new File(websiteLocation, ISSUES_FOLDER), file);
		HttpHeaders headers = new HttpHeaders();
		// headers.add(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment;
		// filename=\"%s\"", fl.getName()));
		headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + fl.getName());
		headers.add(HttpHeaders.CONTENT_TYPE, "application/octet-binary");
		headers.add(HttpHeaders.CONTENT_LENGTH, fl.length() + "");
		return ResponseEntity.ok().headers(headers).body(new FileSystemResource(fl));
	}
	
	static class IssueFile {
		public String name;
		public Date date;
		public long size;

		public IssueFile(String name, long sizeBytes, Date date) {
			this.name = name;
			this.size = sizeBytes;
			this.date = date;
			
		}
	}
    
	  @GetMapping
	  public String index(Model model) throws SQLException, IOException {
		File[] files = new File(websiteLocation, ISSUES_FOLDER).listFiles();
		List<IssueFile> issueFiles = new ArrayList<>();
		if (files != null) {
			for (File f : files) {
				if (f.getName().endsWith(".parquet")) {
					issueFiles.add(new IssueFile(f.getName(), f.length(), new Date(f.lastModified())));
				}
			}
		}
		
		model.addAttribute("issueFiles", issueFiles);
		System.out.println(model.asMap());

		return "admin/issues";
	}
 }
