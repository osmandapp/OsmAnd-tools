package net.osmand.server.controllers.pub;

import net.osmand.server.api.test.entity.EvalJob;
import net.osmand.server.api.services.TestSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;

@Controller
public class TestSearchWSController {

    @Autowired
    private TestSearchService testSearchService;

    @MessageMapping("/eval/ws/{jobId}")
    public EvalJob handleJobUpdates(@DestinationVariable Long jobId) {
        // This method is intended to be a trigger for updates, not necessarily to return data here.
        // The actual updates are pushed from the service layer via SimpMessagingTemplate.
        // However, returning the job can be useful for request-reply patterns if needed.
        return testSearchService.getJob(jobId).orElse(null);
    }
}
