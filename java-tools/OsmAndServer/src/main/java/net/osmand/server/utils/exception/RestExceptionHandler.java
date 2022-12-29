package net.osmand.server.utils.exception;

import com.google.gson.Gson;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

@RestControllerAdvice
public class RestExceptionHandler {
    
    private static final String ERROR_CODE = "errorCode";
    private static final String MESSAGE = "message";
    private static final String ERROR = "error";
    Gson gson = new Gson();
    
    @ExceptionHandler(OsmAndPublicApiException.class)
    public ResponseEntity<String> fileNotAvailable(OsmAndPublicApiException ex) {
        return error(ex.getErrorCode(), ex.getMessage());
    }
    
    private ResponseEntity<String> error(int errorCode, String message) {
        Map<String, Object> mp = new TreeMap<>();
        mp.put(ERROR_CODE, errorCode);
        mp.put(MESSAGE, message);
        return ResponseEntity.badRequest().body(gson.toJson(Collections.singletonMap(ERROR, mp)));
    }
}
