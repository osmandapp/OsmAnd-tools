package net.osmand.server.controllers.pub;

import net.osmand.server.api.services.BuildDeltaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.servlet.http.HttpServletRequest;

import java.io.File;
import java.io.IOException;
import java.util.List;

/** A phone sends the manifest of its APK and downloads only the difference to a build. */
@RestController
@RequestMapping("/api/builds/delta")
public class BuildDeltaController {

	@Autowired
	private BuildDeltaService deltaService;

	/** What the newest builds would take from a phone with this manifest; answers at once. */
	@PostMapping("/sizes")
	public List<BuildDeltaService.BuildSize> getSizes(HttpServletRequest request) throws IOException {
		return deltaService.getSizes(request.getInputStream());
	}

	/** 204 means the phone takes the whole APK. */
	@PostMapping
	public ResponseEntity<BuildDeltaService.Delta> createDelta(@RequestParam String path, HttpServletRequest request)
			throws IOException {
		try {
			BuildDeltaService.Delta delta = deltaService.getDelta(path, request.getInputStream());
			return delta == null ? ResponseEntity.noContent().build() : ResponseEntity.ok(delta);
		} catch (BuildDeltaService.TooManyRequestsException e) {
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
		} catch (IllegalArgumentException e) {
			return ResponseEntity.notFound().build();
		}
	}

	@GetMapping("/{key}")
	public ResponseEntity<Resource> getDelta(@PathVariable String key) {
		File delta = deltaService.getDeltaFile(key);
		if (delta == null) {
			return ResponseEntity.notFound().build();
		}
		return ResponseEntity.ok().contentType(MediaType.APPLICATION_OCTET_STREAM).body(new FileSystemResource(delta));
	}
}
