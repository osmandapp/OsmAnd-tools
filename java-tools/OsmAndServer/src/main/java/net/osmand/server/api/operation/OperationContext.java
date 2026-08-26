package net.osmand.server.api.operation;

import java.util.function.Supplier;

public class OperationContext {
	private volatile boolean cancelled;
	private volatile String progressText;
	private volatile int processed;
	private volatile int total;
	private volatile Supplier<Object> resultSupplier;
	private volatile Object extraParams;

	public Object getExtraParams() {
		return extraParams;
	}

	public void setExtraParams(Object extraParams) {
		this.extraParams = extraParams;
	}

	public boolean isCancelled() {
		return cancelled || Thread.currentThread().isInterrupted();
	}

	public void cancel() {
		this.cancelled = true;
	}

	public void setResultSupplier(Supplier<Object> resultSupplier) {
		this.resultSupplier = resultSupplier;
	}

	public Object snapshotResult() {
		Supplier<Object> supplier = resultSupplier;
		return supplier == null ? null : supplier.get();
	}

	public String getProgressText() {
		return progressText;
	}

	public void setProgressText(String progressText) {
		this.progressText = progressText;
	}

	public int getProcessed() {
		return processed;
	}

	public int getTotal() {
		return total;
	}

	public void setProgress(int processed, int total) {
		this.processed = processed;
		this.total = total;
	}

	public void setProgress(int processed, int total, String progressText) {
		this.processed = processed;
		this.total = total;
		this.progressText = progressText;
	}
}
