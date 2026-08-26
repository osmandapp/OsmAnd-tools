package net.osmand.server.api.operation;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.server.ResponseStatusException;

import net.osmand.server.api.operation.OperationRepository.OperationItem;
import net.osmand.server.api.operation.OperationRepository.JobItem;
import net.osmand.server.api.operation.OperationRepository.RunItem;
import net.osmand.server.api.operation.OperationService.JobRequest;
import net.osmand.server.api.operation.OperationService.RunRequest;

@Controller
@RequestMapping(path = "/admin/operations")
public class OperationController {

	@Autowired
	private OperationRepositoryConfiguration dbCfg;
	@Autowired
	private OperationService operationService;

	@GetMapping
	public String index() {
		return "admin/operations";
	}

	@GetMapping(value = "/initialized", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Boolean> initialized() {
		return ResponseEntity.ok(dbCfg.isOperationDataSourceInitialized());
	}

	@GetMapping(value = "/operations", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<OperationItem>> getOperations() {
		return ResponseEntity.ok(operationService.getOperations());
	}

	@GetMapping(value = "/operations/{className}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<OperationItem> getOperation(@PathVariable String className) {
		return operationService.getOperation(className).map(ResponseEntity::ok)
				.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
	}

	@GetMapping(value = "/jobs", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<JobItem>> getJobs() {
		return ResponseEntity.ok(operationService.getJobs());
	}

	@PostMapping(value = "/jobs", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<JobItem> createJob(@RequestBody JobRequest request) {
		return ResponseEntity.ok(operationService.createJob(request));
	}

	@GetMapping(value = "/jobs/{id:\\d+}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<JobItem> getJob(@PathVariable long id) {
		return operationService.getJob(id).map(ResponseEntity::ok)
				.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
	}

	@PutMapping(value = "/jobs/{id:\\d+}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<JobItem> updateJob(@PathVariable long id, @RequestBody JobRequest request) {
		return ResponseEntity.ok(operationService.updateJob(id, request));
	}

	@DeleteMapping(value = "/jobs/{id:\\d+}")
	@ResponseBody
	public ResponseEntity<Void> deleteJob(@PathVariable long id) {
		operationService.deleteJob(id);
		return ResponseEntity.noContent().build();
	}

	@PostMapping(value = "/jobs/{id:\\d+}/runs", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<RunItem> startRun(@PathVariable long id, @RequestBody(required = false) RunRequest request) {
		return ResponseEntity.ok(operationService.startRun(id, request));
	}

	@GetMapping(value = "/jobs/{id:\\d+}/runs", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<RunItem>> getJobRuns(@PathVariable long id) {
		return ResponseEntity.ok(operationService.getRuns(id));
	}

	@GetMapping(value = "/runs", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<RunItem>> getRuns() {
		return ResponseEntity.ok(operationService.getRuns(null));
	}

	@GetMapping(value = "/runs/{id:\\d+}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<RunItem> getRun(@PathVariable long id) {
		return operationService.getRun(id).map(ResponseEntity::ok)
				.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
	}

	@GetMapping(value = "/runs/{id:\\d+}/status", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<RunItem> getRunStatus(@PathVariable long id) {
		return getRun(id);
	}

	@DeleteMapping(value = "/runs/{id:\\d+}")
	@ResponseBody
	public ResponseEntity<Void> deleteRun(@PathVariable long id) {
		operationService.deleteRun(id);
		return ResponseEntity.noContent().build();
	}

	@PostMapping(value = "/runs/{id:\\d+}/cancel", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<RunItem> cancelRun(@PathVariable long id) {
		return ResponseEntity.ok(operationService.cancelRun(id));
	}
}
