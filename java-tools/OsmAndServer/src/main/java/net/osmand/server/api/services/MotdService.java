package net.osmand.server.api.services;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class MotdService {
    private static final Log LOGGER = LogFactory.getLog(MotdService.class);
    private static final String MOTD_SETTINGS = "api/messages/motd_config.json";

    @Value("${web.location}")
    private String websiteLocation;

    private final ObjectMapper mapper;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm");
    static {
    	dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    
    private MotdSettings settings;

    public MotdService() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setDateFormat(dateFormat);
        this.mapper = objectMapper;
    }
    
    

    public MotdSettings getSettings() {
		return settings;
	}

    public HashMap<String,Object> getMessage(String version, String os, String hostAddress) throws IOException, ParseException {
        Date now = new Date();
        HashMap<String,Object> message = null;
		MotdSettings settings = getSettings();
		if (settings != null) {
			for (DiscountSetting setting : settings.discountSettings) {
				if (setting.checkCondition(now, hostAddress, version, os)) {
					message = setting.parseMotdMessageFile(mapper, websiteLocation.concat("api/messages/"));
					break;
				}
			}
		}
        return message;
    }

    @PostConstruct
	public boolean reloadconfig() {
		return reloadconfig(new ArrayList<String>());
	}
    
    public boolean reloadconfig(List<String> errors) {
    	try {
    		this.settings = mapper.readValue(new File(websiteLocation.concat(MOTD_SETTINGS)), MotdSettings.class);
    	} catch (IOException ex) {
    		if(errors != null) {
    			errors.add(MOTD_SETTINGS + " is invalid: " + ex.getMessage());
    		}
            LOGGER.warn(ex.getMessage(), ex);
            return false;
    	}
        return true;
    }


    

    

    public static class MotdSettings {

        @JsonProperty("settings")
        protected List<DiscountSetting> discountSettings = new ArrayList<MotdService.DiscountSetting>();
        
        public List<DiscountSetting> getDiscountSettings() {
			return discountSettings;
		}

    }

    private static   class DiscountSetting {
        @JsonProperty("condition")
        public DiscountCondition discountCondition;
        @JsonProperty("file")
        public String file;
        @JsonProperty("fields")
        public Map<String, String> fields;


        protected HashMap<String,Object> parseMotdMessageFile(ObjectMapper mapper, String folder) throws IOException {
        	if(file != null && file.length() > 0 && new File(folder, file).exists() ) {
        		TypeReference<HashMap<String,Object>> typeRef = new TypeReference<HashMap<String,Object>>() {};
        		HashMap<String,Object> res = mapper.readValue(new File(folder, file), typeRef);
        		if(res != null) {
        			res = modifyMessageIfNeeded(res, discountCondition.startDate, discountCondition.endDate);
        		}
        		return res;
        	}
        	return null;
        }
        protected HashMap<String,Object> modifyMessageIfNeeded(HashMap<String,Object> message, Date start, Date end) {
            if (fields != null) {
            	Iterator<Entry<String, String>> it = fields.entrySet().iterator();
            	while(it.hasNext()) {
            		Entry<String, String> e = it.next();
            		message.put(e.getKey(), e.getValue());
            	}
            }
            if(start != null) {
            	message.put("start", dateFormat.format(start));
            }
            if(start != null) {
            	message.put("end", dateFormat.format(end));
            }
            return message;
        }

        public boolean checkCondition(Date now, String hostAddress, String version, String os) {
			return discountCondition.checkCondition(now, hostAddress, version, os);
		}

    }

    public static class DiscountCondition {
    	@JsonProperty("ip")
    	private String ip;
        @JsonProperty("start_date")
        private Date startDate;
        @JsonProperty("end_date")
        private Date endDate;
        @JsonProperty("version")
        private String version;
        @JsonProperty("os")
        private String os;
        
		public String getFilterCondition() {
			String filter = "";
			if (os != null) {
				filter += " " + os + ": ";
			}
			if (ip != null) {
				filter += " IP in '" + ip + "' ";
			}
			
			if (startDate != null || endDate != null) {
				filter += String.format(" Date between %s and %s", 
						(startDate == null ? "-" : String.format("%1$tF %1$tR", startDate)),
						(endDate == null ? "-" : String.format("%1$tF %1$tR", endDate)));
			}
			if (version != null) {
				filter += " Version in '" + version + "'";
			}
			return filter;

		}
        

        public boolean checkCondition(Date date, String hostAddress, String version, String osV) {
            if (ip != null && !ip.contains(hostAddress)) {
                return false;
            }
            if(!checkActiveDate(date)) {
            	return false;
            }
            if (this.os != null && this.os.length() > 0) {
				String osVersion = osV != null && osV.equals("ios") ? "ios" : "android";
				if(!osVersion.equals(this.os)) {
					return false;
				}
            }
            if (this.version != null && (version == null || !version.startsWith(this.version))) {
                return false;
            }
            return true;
        }

		public boolean checkActiveDate(Date date) {
			if (startDate != null && !date.after(startDate)) {
            	return false;
            }
            if (endDate != null && !date.before(endDate)) {
            	return false;
            }
            return true;
		}
    }
}
