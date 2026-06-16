package net.osmand.server.api.operation;

public class OperationContext {
	private volatile boolean cancelled;
	private volatile String progressText;
	private volatile int processed;
	private volatile int total;

	public boolean isCancelled() {
		return cancelled || Thread.currentThread().isInterrupted();
	}

	public void cancel() {
		this.cancelled = true;
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
