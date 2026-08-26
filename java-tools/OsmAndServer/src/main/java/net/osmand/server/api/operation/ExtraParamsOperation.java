package net.osmand.server.api.operation;

/**
 * Lets an operation declare its own params (a record) that are merged with the base params in the UI
 * and passed to run() via {@link OperationContext#getExtraParams()}.
 */
public interface ExtraParamsOperation {
	Class<?> extraParamsType();
}
