package net.osmand.server.api.action.bean;

import net.osmand.server.api.action.Action;
import net.osmand.server.api.action.UiAction;
import net.osmand.server.api.action.UiParam;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@UiAction(name = "list-action", title = "Long text")
public class ListTestAction implements Action {
    public List<Map<String, Object>> run(@UiParam(name = "param1", defaultValue = "val1", title = "help description") String input, int limit) {
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(Map.of("input1", input, "limit1", limit));
        list.add(Map.of("input2", input, "limit2", limit));
        return list;
    }
}
