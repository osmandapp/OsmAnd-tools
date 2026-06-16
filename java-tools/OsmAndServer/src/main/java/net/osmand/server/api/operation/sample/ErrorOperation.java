package net.osmand.server.api.operation.sample;

import org.springframework.stereotype.Component;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.operation.Operation;
import net.osmand.server.api.operation.OperationContext;

@Component
@AdminOperation(name = "error-operation", title = "Error sample")
public class ErrorOperation implements Operation<ErrorOperation.Params> {

	public record Params(String input, int limit) {}

	@Override
	public Object run(Params params, OperationContext context) {
		throw new RuntimeException("Something goes wrong.");
	}
}
