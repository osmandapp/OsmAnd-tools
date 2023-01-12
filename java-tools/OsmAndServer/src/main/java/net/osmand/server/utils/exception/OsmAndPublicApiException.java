package net.osmand.server.utils.exception;

public class OsmAndPublicApiException extends RuntimeException {
    private final int errorCode;
    
    public OsmAndPublicApiException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }
    
    public int getErrorCode() {
        return errorCode;
    }
}
