package net.osmand.server.api.services;

import java.io.File;
import java.io.FileReader;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

@Service
public class PollsService {
    private static final Log LOGGER = LogFactory.getLog(PollsService.class);

    @Value("${web.location}")
    private String websiteLocation;
    
    Gson gson = new Gson();
    
    private Map<String, PollQuestion> polls;
    
    public static class PollQuestion {
    	public String title;
    	public String polldaddyid;
    	public String pubdate;
    	
    }
    
    public void reloadConfigs(List<String> errors) {
    	try { 
    		getPollsConfig(true);
    	} catch(Exception e) {
    		LOGGER.warn(e.getMessage(), e);
    		errors.add("Error while reloading polls configuration " + e.getMessage());
    	}
    }
    
    @SuppressWarnings("unchecked")
	public Map<String, PollQuestion> getPollsConfig(boolean reload) {
    	if(reload || polls == null) {
    		try {
    			polls = new LinkedHashMap<>();
    			File pollsConfig = new File(websiteLocation, "api/poll.json");
    			polls = gson.fromJson(new FileReader(pollsConfig), polls.getClass());
    		} catch(Exception e) {
        		LOGGER.warn(e.getMessage(), e);
        	}
    	} 
    	return polls;
    }
}