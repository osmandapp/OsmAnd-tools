package net.osmand.server.controllers.user;

import net.osmand.mailsender.EmailTemplatePreview;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Controller
@RequestMapping("/admin/email-templates")
public class EmailTemplatesController {

	private static final Pattern TEMPLATE_ID = Pattern.compile("[a-z0-9-]+(/[a-z0-9-]+)+");

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

	@PostMapping("/render-markup")
	@ResponseBody
	public ResponseEntity<?> renderMarkup(@RequestParam(defaultValue = "") String id, @RequestParam String html,
	                                      @RequestParam(defaultValue = "") String texts,
	                                      @RequestParam(defaultValue = "") String params,
	                                      @RequestParam(defaultValue = "") String styles,
	                                      @RequestParam(defaultValue = "") String headers,
	                                      @RequestParam(required = false) String lang) {
		String templateId = id.trim();
		if (!TEMPLATE_ID.matcher(templateId).matches()) {
			return ResponseEntity.badRequest().body("Template id is the path inside emails/ without .html, e.g. marketing/summer-sale");
		}
		EmailTemplatePreview.Draft draft = new EmailTemplatePreview().renderMarkup(templateId, html, texts, params, styles, headers, lang);
		String filled = String.join("\n", draft.subject(), draft.html(), String.join("\n", draft.headers().values()));
		return ResponseEntity.ok(Map.of("html", draft.html(), "missing", EmailTemplatePreview.missingVariables(filled)));
	}

	private static Map<String, Object> withMissing(String html) {
		return Map.of("html", html, "missing", EmailTemplatePreview.missingVariables(html));
	}
}
