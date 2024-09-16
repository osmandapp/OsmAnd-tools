package net.osmand.server.utils.exception;

public class OsmAndPublicApiException extends RuntimeException {
    private static final long serialVersionUID = 3621593358522427550L;
	private final int errorCode;
    
    public OsmAndPublicApiException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
        System.err.printf("OsmAndPublicApiException (%d):\n%s\n", errorCode, message);
    }
    
    public int getErrorCode() {
        return errorCode;
    }
}
