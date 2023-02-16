package net.osmand.server.utils.exception;

import com.google.gson.Gson;

import net.osmand.server.controllers.pub.DownloadIndexController;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

@RestControllerAdvice
public class RestExceptionHandler {

	private static final Log LOGGER = LogFactory.getLog(RestExceptionHandler.class);

    private static final String ERROR_CODE = "errorCode";
    private static final String MESSAGE = "message";
    private static final String ERROR = "error";
    Gson gson = new Gson();
    
    @ExceptionHandler(OsmAndPublicApiException.class)
    public ResponseEntity<String> fileNotAvailable(OsmAndPublicApiException ex) {
        return error(ex.getErrorCode(), ex.getMessage());
    }
    
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<String> genericException(RuntimeException ex) {
    	LOGGER.error(ex.getMessage(), ex);
        throw ex;
    }
    
    private ResponseEntity<String> error(int errorCode, String message) {
        Map<String, Object> mp = new TreeMap<>();
        mp.put(ERROR_CODE, errorCode);
        mp.put(MESSAGE, message);
        return ResponseEntity.badRequest().body(gson.toJson(Collections.singletonMap(ERROR, mp)));
    }
}
