package net.osmand.server.controllers.user;

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import jakarta.servlet.http.HttpServletResponse;
import net.osmand.server.api.services.TurnLanesBuilds;
import net.osmand.server.api.services.TurnLanesCompareService;
import net.osmand.server.api.services.TurnLanesCompareService.Compare;
import net.osmand.server.api.services.TurnLanesCompareService.CompareRequest;
import net.osmand.server.api.services.TurnLanesReviewService;
import net.osmand.server.api.services.TurnLanesReviewService.VerdictRequest;
import net.osmand.server.api.services.TurnLanesService;
import net.osmand.server.api.services.TurnLanesService.Dataset;
import net.osmand.server.api.services.TurnLanesService.GenerateRequest;
import net.osmand.server.api.services.TurnLanesService.ObfFile;
import net.osmand.tester.GenerateTurnLanesTest;

@Controller
@RequestMapping(path = "/admin/turn-lanes")
public class TurnLanesController {

	@Autowired
	private TurnLanesService service;

	@Autowired
	private TurnLanesCompareService compares;

	@Autowired
	private TurnLanesBuilds builds;

	@Autowired
	private TurnLanesReviewService reviews;

	@ExceptionHandler(IllegalArgumentException.class)
	@ResponseBody
	public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
		return ResponseEntity.badRequest().body(Map.of("message", String.valueOf(e.getMessage())));
	}

	@GetMapping
	public String index() {
		return "admin/turn-lanes";
	}

	@GetMapping(value = "/defaults", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public Map<String, Object> defaults() {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("obfDir", service.getDefaultObfDir());
		m.put("build", service.getBuild());
		m.put("location", service.getRoot().getAbsolutePath());
		return m;
	}

	@GetMapping(value = "/obfs", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public List<ObfFile> obfs(@RequestParam String obfDir, @RequestParam(required = false) String prefixes) {
		return service.listObfs(obfDir, prefixes);
	}

	@GetMapping(value = "/datasets", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public List<Dataset> datasets() {
		return service.listDatasets();
	}

	@PostMapping(value = "/datasets", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public Dataset generate(@RequestBody GenerateRequest req) throws IOException {
		return service.generate(req);
	}

	@GetMapping(value = "/datasets/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Dataset> dataset(@PathVariable String name) {
		Dataset d = service.getDataset(name);
		return d == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(d);
	}

	public record LabelsRequest(String labels) {
	}

	@PutMapping(value = "/datasets/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Dataset> updateLabels(@PathVariable String name, @RequestBody LabelsRequest req)
			throws IOException {
		Dataset d = service.updateLabels(name, req.labels());
		return d == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(d);
	}

	@PostMapping(value = "/datasets/{name}/cancel")
	@ResponseBody
	public ResponseEntity<Void> cancel(@PathVariable String name) {
		return service.cancel(name) ? ResponseEntity.ok().build() : ResponseEntity.status(HttpStatus.CONFLICT).build();
	}

	@DeleteMapping(value = "/datasets/{name}")
	@ResponseBody
	public ResponseEntity<Void> delete(@PathVariable String name) throws IOException {
		if (compares.isActive(name)) {
			return ResponseEntity.status(HttpStatus.CONFLICT).build();
		}
		return service.delete(name) ? ResponseEntity.ok().build() : ResponseEntity.status(HttpStatus.CONFLICT).build();
	}

	@GetMapping(value = "/datasets/{name}/rows", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public List<Map<String, String>> rows(@PathVariable String name,
	                                      @RequestParam(defaultValue = "500") int limit) throws IOException {
		return service.readRows(name, Math.min(Math.max(limit, 1), 5000));
	}

	@GetMapping(value = "/datasets/{name}/csv")
	public void csv(@PathVariable String name, HttpServletResponse response) throws IOException {
		sendCsv(service.getCasesFile(name), name + ".csv", response);
	}

	// ---------------------------------------------------------------- compare

	/** what a dataset can be replayed on: the night builds there are, and the ones already downloaded */
	@GetMapping(value = "/builds", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public Map<String, Object> builds() {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("current", service.getBuild());
		m.put("latest", List.of("main", "test"));
		try {
			m.put("daily", builds.dailyBuilds());
		} catch (IOException e) {
			m.put("daily", List.of());
			m.put("dailyError", e.getMessage());
		}
		m.put("cached", builds.cached());
		return m;
	}

	@GetMapping(value = "/compares", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public List<Compare> compares(@RequestParam(required = false) String dataset) {
		return compares.list(dataset);
	}

	@PostMapping(value = "/compares", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public Compare compare(@RequestBody CompareRequest req) throws IOException {
		return compares.start(req);
	}

	@GetMapping(value = "/compares/{dataset}/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Compare> compare(@PathVariable String dataset, @PathVariable String id) {
		Compare c = compares.get(dataset, id);
		return c == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(c);
	}

	@PutMapping(value = "/compares/{dataset}/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Compare> updateCompareLabels(@PathVariable String dataset, @PathVariable String id,
	                                                   @RequestBody LabelsRequest req) throws IOException {
		Compare c = compares.updateLabels(dataset, id, req.labels());
		return c == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(c);
	}

	@PostMapping(value = "/compares/{dataset}/{id}/cancel")
	@ResponseBody
	public ResponseEntity<Void> cancelCompare(@PathVariable String dataset, @PathVariable String id) {
		return compares.cancel(dataset, id) ? ResponseEntity.ok().build()
				: ResponseEntity.status(HttpStatus.CONFLICT).build();
	}

	@DeleteMapping(value = "/compares/{dataset}/{id}")
	@ResponseBody
	public ResponseEntity<Void> deleteCompare(@PathVariable String dataset, @PathVariable String id)
			throws IOException {
		return compares.delete(dataset, id) ? ResponseEntity.ok().build()
				: ResponseEntity.status(HttpStatus.CONFLICT).build();
	}

	/** {@code status}: comma separated verdicts to keep, all when empty */
	@GetMapping(value = "/compares/{dataset}/{id}/rows", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public List<Map<String, String>> compareRows(@PathVariable String dataset, @PathVariable String id,
	                                             @RequestParam(required = false) String status,
	                                             @RequestParam(defaultValue = "2000") int limit) throws IOException {
		Set<String> verdicts = new HashSet<>();
		if (status != null) {
			Arrays.stream(status.split(",")).map(String::trim).filter(v -> !v.isEmpty()).forEach(verdicts::add);
		}
		List<Map<String, String>> rows = compares.readRows(dataset, id, verdicts, Math.min(Math.max(limit, 1), 10000));
		// what the manual check said about each, for the list to show
		reviews.addVerdicts(dataset, id, rows);
		return rows;
	}

	@GetMapping(value = "/compares/{dataset}/{id}/csv")
	public void compareCsv(@PathVariable String dataset, @PathVariable String id, HttpServletResponse response)
			throws IOException {
		sendCsv(compares.getResultFile(dataset, id), dataset + "_" + id + ".csv", response);
	}

	// ---------------------------------------------------------------- manual check

	private static String userOf(Principal p) {
		return p == null || p.getName() == null ? "" : p.getName();
	}

	private static void sendCsv(File f, String name, HttpServletResponse response) throws IOException {
		if (f == null || !f.exists()) {
			response.sendError(HttpStatus.NOT_FOUND.value());
			return;
		}
		response.setContentType("text/csv; charset=UTF-8");
		response.setHeader("Content-Disposition", "attachment; filename=\"" + name + "\"");
		response.setContentLengthLong(f.length());
		Files.copy(f.toPath(), response.getOutputStream());
	}

	@GetMapping(value = "/review/{dataset}/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Map<String, Object>> review(@PathVariable String dataset, @PathVariable String id)
			throws IOException {
		Compare c = compares.get(dataset, id);
		List<Map<String, Object>> rows = c == null ? null : reviews.rows(dataset, id);
		if (rows == null) {
			return ResponseEntity.notFound().build();
		}
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("compare", c);
		Dataset d = service.getDataset(dataset);
		m.put("profile", (d == null || d.params == null ? new GenerateTurnLanesTest.Options() : service.options(d.params)).profile);
		m.put("rows", rows);
		return ResponseEntity.ok(m);
	}

	@PostMapping(value = "/review/{dataset}/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public Map<String, Object> verdict(@PathVariable String dataset, @PathVariable String id,
	                                   @RequestBody VerdictRequest req, Principal user) throws IOException {
		return reviews.verdict(dataset, id, req, userOf(user));
	}

	@GetMapping(value = "/review/{dataset}/{id}/csv")
	public void reviewCsv(@PathVariable String dataset, @PathVariable String id, HttpServletResponse response)
			throws IOException {
		sendCsv(reviews.getReviewFile(dataset, id), dataset + "_" + id + "_review.csv", response);
	}

	@GetMapping(value = "/common/csv")
	public void commonCsv(HttpServletResponse response) throws IOException {
		sendCsv(reviews.getCommonFile(), TurnLanesReviewService.COMMON, response);
	}

	public record CaseRequest(String num) {
	}

	/** one drive as test_turn_lanes.json */
	@PostMapping(value = "/review/{dataset}/{id}/case", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public Map<String, Object> caseJson(@PathVariable String dataset, @PathVariable String id,
	                                    @RequestBody CaseRequest req) throws IOException {
		return reviews.caseJson(dataset, id, req.num());
	}
}
