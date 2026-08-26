package net.osmand.server.api.operation;

public interface Operation<P> {
	Object run(P params, OperationContext context);
}
