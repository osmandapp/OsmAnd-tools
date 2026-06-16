package net.osmand.server.api.operation.sample;

import java.util.Map;

import org.springframework.stereotype.Component;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.operation.Operation;
import net.osmand.server.api.operation.OperationContext;

@Component
@AdminOperation(name = "sample-operation", title = "Sample")
public class SampleOperation implements Operation<SampleOperation.Params> {

	public record Params(String input, int limit) {}

	@Override
	public Object run(Params params, OperationContext context) {
		return Map.of("input", params.input(), "limit", params.limit());
	}
}
