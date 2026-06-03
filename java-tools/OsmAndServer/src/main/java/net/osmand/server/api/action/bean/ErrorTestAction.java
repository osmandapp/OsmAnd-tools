package net.osmand.server.api.action.bean;

import net.osmand.server.api.action.Action;
import net.osmand.server.api.action.UiAction;
import net.osmand.server.api.action.UiParam;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@UiAction(name = "error-action", title = "Long text")
public class ErrorTestAction implements Action {
    public Map<String, Object> run(@UiParam(name = "param1", defaultValue = "val1", title = "help description") String input, int limit) {
        throw new RuntimeException("Something goes wrong.");
    }
}
