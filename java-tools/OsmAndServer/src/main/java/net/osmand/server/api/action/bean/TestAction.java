package net.osmand.server.api.action.bean;

import net.osmand.server.api.action.Action;
import net.osmand.server.api.action.UiAction;
import net.osmand.server.api.action.UiParam;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@UiAction(name = "sample-action", title = "Sample Action")
public class TestAction implements Action {
    public Map<String, Object> run(@UiParam(name = "param1", defaultValue = "val1", title = "help description") String input, int limit) {
        return Map.of("input", input, "limit", limit);
    }
}
