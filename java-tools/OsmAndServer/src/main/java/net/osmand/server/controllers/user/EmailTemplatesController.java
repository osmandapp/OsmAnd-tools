package net.osmand.server.controllers.user;

import net.osmand.mailsender.EmailTemplatePreview;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/admin/email-templates")
public class EmailTemplatesController {

	@GetMapping(path = { "", "/" })
	public String emailTemplatesPage() {
		return "admin/email-templates";
	}

	@GetMapping(value = "/list", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public Map<String, List<String>> list() throws IOException {
		EmailTemplatePreview preview = new EmailTemplatePreview();
		return Map.of("templates", preview.templateIds(), "languages", preview.languages());
	}

	@GetMapping(value = "/render", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Map<String, Object>> render(@RequestParam String id, @RequestParam(required = false) String lang)
			throws IOException {
		EmailTemplatePreview preview = new EmailTemplatePreview();
		if (!preview.templateIds().contains(id)) {
			return ResponseEntity.notFound().build();
		}
		return ResponseEntity.ok(withMissing(preview.render(id, lang)));
	}

	@PostMapping(value = "/render-markup", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public Map<String, Object> renderMarkup(@RequestParam String html, @RequestParam(defaultValue = "") String values,
	                                        @RequestParam(required = false) String lang) {
		return withMissing(new EmailTemplatePreview().renderMarkup(html, values, lang));
	}

	private static Map<String, Object> withMissing(String html) {
		return Map.of("html", html, "missing", EmailTemplatePreview.missingVariables(html));
	}
}
