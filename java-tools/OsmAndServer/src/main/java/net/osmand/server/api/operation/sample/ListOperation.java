package net.osmand.server.api.operation.sample;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.operation.Operation;
import net.osmand.server.api.operation.OperationContext;

@Component
@AdminOperation(name = "list-operation", title = "List sample")
public class ListOperation implements Operation<ListOperation.Params> {

	public record Params(String input, int limit) {}

	@Override
	public Object run(Params params, OperationContext context) {
		List<Map<String, Object>> list = new ArrayList<>();
		list.add(Map.of("input1", params.input(), "limit1", params.limit()));
		list.add(Map.of("input2", params.input(), "limit2", params.limit()));
		return list;
	}
}
