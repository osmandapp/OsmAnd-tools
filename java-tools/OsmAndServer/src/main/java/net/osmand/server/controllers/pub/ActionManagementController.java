package net.osmand.server.controllers.pub;

import net.osmand.server.ActionRepositoryConfiguration;
import net.osmand.server.api.services.ActionManagementService;
import net.osmand.server.api.services.ActionManagementService.ActionItem;
import net.osmand.server.api.services.ActionManagementService.JobItem;
import net.osmand.server.api.services.ActionManagementService.JobRequest;
import net.osmand.server.api.services.ActionManagementService.RunItem;
import net.osmand.server.api.services.ActionManagementService.RunRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

import java.util.List;

@Controller
@RequestMapping(path = "/admin/action-management")
public class ActionManagementController {

    @Autowired
    private ActionRepositoryConfiguration dbCfg;
    @Autowired
    private ActionManagementService actionManagementService;

    @GetMapping
    public String index(Model model) {
        return "admin/action-management";
    }

    @GetMapping(value = "/initialized", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<Boolean> initialized() {
        return ResponseEntity.ok(dbCfg.isActionDataSourceInitialized());
    }

    @GetMapping(value = "/actions", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<List<ActionItem>> getActions() {
        return ResponseEntity.ok(actionManagementService.getActions());
    }

    @GetMapping(value = "/actions/{className}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<ActionItem> getAction(@PathVariable String className) {
        return actionManagementService.getAction(className).map(ResponseEntity::ok)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    @GetMapping(value = "/jobs", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<List<JobItem>> getJobs() {
        return ResponseEntity.ok(actionManagementService.getJobs());
    }

    @PostMapping(value = "/jobs", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<JobItem> createJob(@RequestBody JobRequest request) {
        return ResponseEntity.ok(actionManagementService.createJob(request));
    }

    @GetMapping(value = "/jobs/{id:\\d+}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<JobItem> getJob(@PathVariable long id) {
        return actionManagementService.getJob(id).map(ResponseEntity::ok)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    @PutMapping(value = "/jobs/{id:\\d+}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<JobItem> updateJob(@PathVariable long id, @RequestBody JobRequest request) {
        return ResponseEntity.ok(actionManagementService.updateJob(id, request));
    }

    @DeleteMapping(value = "/jobs/{id:\\d+}")
    @ResponseBody
    public ResponseEntity<Void> deleteJob(@PathVariable long id) {
        actionManagementService.deleteJob(id);
        return ResponseEntity.noContent().build();
    }

    @PostMapping(value = "/jobs/{id:\\d+}/runs", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<RunItem> startRun(@PathVariable long id, @RequestBody(required = false) RunRequest request) {
        return ResponseEntity.ok(actionManagementService.startRun(id, request));
    }

    @GetMapping(value = "/jobs/{id:\\d+}/runs", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<List<RunItem>> getJobRuns(@PathVariable long id) {
        return ResponseEntity.ok(actionManagementService.getRuns(id));
    }

    @GetMapping(value = "/runs", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<List<RunItem>> getRuns() {
        return ResponseEntity.ok(actionManagementService.getRuns(null));
    }

    @GetMapping(value = "/runs/{id:\\d+}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<RunItem> getRun(@PathVariable long id) {
        return actionManagementService.getRun(id).map(ResponseEntity::ok)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    @GetMapping(value = "/runs/{id:\\d+}/status", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<RunItem> getRunStatus(@PathVariable long id) {
        return getRun(id);
    }

    @PostMapping(value = "/runs/{id:\\d+}/cancel", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<RunItem> cancelRun(@PathVariable long id) {
        return ResponseEntity.ok(actionManagementService.cancelRun(id));
    }
}
