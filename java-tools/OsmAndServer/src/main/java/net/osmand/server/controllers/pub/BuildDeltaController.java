package net.osmand.server.controllers.pub;

import jakarta.servlet.http.HttpServletRequest;
import net.osmand.server.api.services.BuildDeltaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.util.List;

/** A phone sends the manifest of its APK and downloads only the difference to a build. */
@RestController
@RequestMapping("/api/builds/delta")
public class BuildDeltaController {

	@Autowired
	private BuildDeltaService deltaService;

	/** What the builds would take from a phone with this manifest; answers at once. */
	@PostMapping("/sizes")
	public List<BuildDeltaService.BuildSize> getSizes(@RequestParam List<String> path, HttpServletRequest request)
			throws IOException {
		return deltaService.getSizes(path, request.getInputStream());
	}

	/** Waits for the delta; 204 means the phone takes the whole APK. */
	@PostMapping
	public ResponseEntity<Resource> getDelta(@RequestParam String path, HttpServletRequest request) throws IOException {
		File delta = deltaService.getDelta(path, request.getInputStream());
		return delta == null ? ResponseEntity.noContent().build()
				: ResponseEntity.ok().contentType(MediaType.APPLICATION_OCTET_STREAM).body(new FileSystemResource(delta));
	}
}
