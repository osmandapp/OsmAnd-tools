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
import org.springframework.beans.factory.annotation.Autowired;
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
    private static final String SUBSCRIPTION_SETTINGS = "api/subscriptions/config.json";

    @Value("${web.location}")
    private String websiteLocation;
    

    @Autowired
	private IpLocationService locationService;

    private final ObjectMapper mapper;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm");
    static {
    	dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    
    private MotdSettings settings;
    private MotdSettings subscriptionSettings;

    public MotdService() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setDateFormat(dateFormat);
        this.mapper = objectMapper;
    }
    
    
    public MotdSettings getSubscriptionSettings() {
		return subscriptionSettings;
	}
    

    public MotdSettings getSettings() {
		return settings;
	}

    
    public HashMap<String,Object> getMessage(String appVersion, 
    		String version, String os, String hostAddress, String lang) throws IOException, ParseException {
        Date now = new Date();
        HashMap<String,Object> message = null;
		MotdSettings settings = getSettings();
		if (settings != null) {
			for (DiscountSetting setting : settings.discountSettings) {
				if (setting.discountCondition.checkCondition(now, hostAddress, 
						appVersion, version, os, lang, "", locationService)) {
					message = setting.parseMotdMessageFile(mapper, websiteLocation.concat("api/messages/"));
					break;
				}
			}
		}
        return message;
    }
    
    public String getSubscriptions(String appVersion, String version, String os, String hostAddress, String lang, String androidPackage)
			throws IOException, ParseException {
		Date now = new Date();
		MotdSettings settings = getSubscriptionSettings();
		if (settings != null) {
			for (DiscountSetting setting : settings.discountSettings) {
				if (setting.discountCondition.checkCondition(now, hostAddress, appVersion, version, os, lang,
						androidPackage, locationService)) {
					return websiteLocation.concat("api/subscriptions/" + setting.file);
				}
			}
		}
		return websiteLocation.concat("api/subscriptions/empty.json");
	}

    @PostConstruct
	public boolean reloadconfig() {
		return reloadconfig(new ArrayList<String>());
	}
    
    public boolean reloadconfig(List<String> errors) {
    	try {
    		this.settings = mapper.readValue(new File(websiteLocation.concat(MOTD_SETTINGS)), MotdSettings.class);
    		this.subscriptionSettings = mapper.readValue(new File(websiteLocation.concat(SUBSCRIPTION_SETTINGS)), MotdSettings.class);
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

    private static class DiscountSetting {
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
        @JsonProperty("app_version")
        private String appVersion;
        @JsonProperty("app_package")
        private String appPackage;
        
        
        @JsonProperty("lang")
        private String lang;
        @JsonProperty("country")
        private String country;
        @JsonProperty("city")
        private String city;
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
			if (lang != null) {
				filter += " Language is '" + lang + "' ";
			}
			if (country != null) {
				filter += " Country is '" + country + "' ";
			}
			if (city != null) {
				filter += " City is '" + city + "' ";
			}
			
			if (startDate != null || endDate != null) {
				filter += String.format(" Date between %s and %s", 
						(startDate == null ? "-" : String.format("%1$tF %1$tR", startDate)),
						(endDate == null ? "-" : String.format("%1$tF %1$tR", endDate)));
			}
			if (appPackage != null) {
				filter += " appPackage '" + appVersion + "'";
			}
			if (appVersion != null) {
				filter += " AppVersion '" + appVersion + "'";
			}
			if (version != null) {
				filter += " Version in '" + version + "'";
			}
			return filter;

		}
        

        public boolean checkCondition(Date date, String hostAddress, 
        		String appVersion, String version, String osV, String lang, 
        		String appPackage, IpLocationService locationService) {
            if (ip != null && !ip.contains(hostAddress)) {
                return false;
            }
            if(!checkActiveDate(date)) {
            	return false;
            }
            if(country != null) {
				String cnt = locationService.getField(hostAddress, IpLocationService.COUNTRY_NAME);
            	if(!cnt.equalsIgnoreCase(country)) {
            		return false;
            	}
            }
            if(city != null) {
				String cnt = locationService.getField(hostAddress, IpLocationService.CITY);
            	if(!cnt.equalsIgnoreCase(city)) {
            		return false;
            	}
            }
            if (this.os != null && this.os.length() > 0) {
				String osVersion = osV != null && osV.equals("ios") ? "ios" : "android";
				if(!osVersion.equals(this.os)) {
					return false;
				}
            }
            if (this.appPackage != null && (appPackage == null || !appPackage.equalsIgnoreCase(this.appPackage))) {
                return false;
            }
            if (this.appVersion != null && (appVersion == null || !appVersion.equalsIgnoreCase(this.appVersion))) {
                return false;
            }
            if (this.version != null && (version == null || !version.startsWith(this.version))) {
                return false;
            }
            if (this.lang != null && (lang == null || !this.lang.contains(lang))) {
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
