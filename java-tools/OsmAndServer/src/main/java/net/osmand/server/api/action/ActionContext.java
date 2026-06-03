package net.osmand.server.api.action;

public class ActionContext {
    private volatile boolean cancelled;
    private volatile String progressText;

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
}
