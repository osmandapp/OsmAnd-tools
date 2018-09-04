package net.osmand.server.services.api;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class MotdService {
    private static final Log LOGGER = LogFactory.getLog(MotdService.class);
    private static final String MOTD_SETTINGS = "api/messages/motd_config.json";
    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm";

    @Value("${web.location}")
    private String websiteLocation;

    private final ObjectMapper mapper;
    
    private MotdSettings settings;

    public MotdService() {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setDateFormat(dateFormat);
        this.mapper = objectMapper;
    }
    
    

    public MotdSettings getSettings() {
		return settings;
	}

    public MotdMessage getMessage(String version, String os, String hostAddress) throws IOException, ParseException {
        Date now = new Date();
        MotdMessage message = null;
		MotdSettings settings = getSettings();
		if (settings != null) {
			for (DiscountSetting setting : settings.discountSettings) {
				if (setting.checkCondition(now, hostAddress, version)) {
					String filename = setting.getMotdFileByPlatform(os);
					message = parseMotdMessageFile(websiteLocation.concat("api/messages/").concat(filename));
					if (message != null) {
						message = setting.modifyMessageIfNeeded(message);
					}
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


    

    private MotdMessage parseMotdMessageFile(String filepath) throws IOException {
    	if(filepath != null && filepath.length() > 0 && new File(filepath).exists() ) {
    		return mapper.readValue(new File(filepath), MotdMessage.class);
    	}
    	return null;
    }

    public static class MotdSettings {

        @JsonProperty("settings")
        protected List<DiscountSetting> discountSettings = new ArrayList<MotdService.DiscountSetting>();
        
        public List<DiscountSetting> getDiscountSettings() {
			return discountSettings;
		}

    }

    private static class DiscountSetting {
        @JsonProperty("condition")
        public DiscountCondition discountCondition;
        @JsonProperty("file")
        public String file;
        @JsonProperty("ios_file")
        public String iosFile;
        @JsonProperty("fields")
        public Map<String, String> fields;


        protected MotdMessage modifyMessageIfNeeded(MotdMessage message) {
            if (fields != null) {
            	if(fields.containsKey("description")) {
            		message.setDescription(fields.get("description"));
            	}
            	if(fields.containsKey("message")) {
            		message.setMessage(fields.get("message"));
            	}
            	if(fields.containsKey("start")) {
            		message.setStartDate(fields.get("start"));
            	}
            	if(fields.containsKey("end")) {
            		message.setEndDate(fields.get("end"));
            	}
            }
            return message;
        }

        public boolean checkCondition(Date now, String hostAddress, String version) {
			return discountCondition.checkCondition(now, hostAddress, version);
		}

		public String getMotdFileByPlatform(String os) {
            if (os != null && os.equals("ios") && iosFile != null) {
                return iosFile;
            }
            return file;
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
        
		public String getFilterCondition() {
			String filter = "";
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
        

        public boolean checkCondition(Date date, String hostAddress, String version) {
            if (ip != null && !ip.contains(hostAddress)) {
                return false;
            }
            if(!checkActiveDate(date)) {
            	return false;
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
