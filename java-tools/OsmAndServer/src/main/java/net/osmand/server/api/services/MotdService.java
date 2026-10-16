package net.osmand.server.api.services;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.annotation.PostConstruct;
import net.osmand.util.Algorithms;

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
    private static final String MOTD_SETTINGS = "api/messages/_config.json";
    private static final String SUBSCRIPTION_SETTINGS = "api/subscriptions/_config.json";

    @Value("${osmand.web.location}")
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

	public static class MessageParams {
		private static final Pattern APP_VERSION = Pattern.compile("^(\\D+) (\\d.*)$");

		public Date now = new Date();
		public String appVersion;
		public String appPackage;
		public String hostAddress;
		public String os;
		public String version;
		public String lang;
		public List<String> features = Collections.emptyList();
		public Integer numberOfDays;
		public Integer numberOfStarts;

		public void setVersion(String fullVersion) {
			appVersion = "";
			version = fullVersion;
			if (fullVersion != null) {
				// Android includes the app name (possibly multiple words); iOS sends only the version.
				Matcher matcher = APP_VERSION.matcher(fullVersion);
				if (matcher.matches()) {
					appVersion = matcher.group(1);
					version = matcher.group(2);
				}
			}
		}
	}
    
    public HashMap<String,Object> getMessage(MessageParams params) throws IOException, ParseException {
        
        HashMap<String,Object> message = null;
		MotdSettings settings = getSettings();
		if (settings != null) {
			for (DiscountSetting setting : settings.discountSettings) {
				if (setting.discountCondition.checkCondition(params, locationService)) {
					message = setting.parseMotdMessageFile(mapper, 
							new File(websiteLocation, "api/messages/"));
					break;
				}
			}
		}
        return message;
    }
    
    public File getSubscriptions(MessageParams params)
			throws IOException, ParseException {
		MotdSettings settings = getSubscriptionSettings();
		if (settings != null) {
			for (DiscountSetting setting : settings.discountSettings) {
				if (setting.discountCondition.checkCondition(params, locationService)) {
					return new File(websiteLocation, "api/subscriptions/" + setting.file);
				}
			}
		}
		return new File(websiteLocation, "api/subscriptions/empty.json");
	}

    @PostConstruct
	public boolean reloadconfig() {
		return reloadconfig(new ArrayList<String>());
	}
    
    public boolean reloadconfig(List<String> errors) {
    	try {
    		this.settings = mapper.readValue(new File(websiteLocation, MOTD_SETTINGS), MotdSettings.class);
    		this.settings.prepare();
    		this.subscriptionSettings = mapper.readValue(new File(websiteLocation, SUBSCRIPTION_SETTINGS), MotdSettings.class);
    		this.subscriptionSettings.prepare();
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
        
        public void prepare() {
        	for(DiscountSetting s : discountSettings) {
        		if(s.discountCondition != null) {
        			s.discountCondition.prepare();
        		}
        	}
        }
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
        
        
        protected HashMap<String,Object> parseMotdMessageFile(ObjectMapper mapper, File folder) throws IOException {
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
        @JsonProperty("number_of_starts")
        private String numberOfStarts;
        @JsonProperty("number_of_days")
        private String numberOfDays;
        @JsonProperty("features")
        private String paid_features;
        
        
        @JsonProperty("lang")
        private String lang;
        @JsonProperty("country")
        private String country;
        @JsonProperty("city")
        private String city;
        @JsonProperty("os")
        private String os;
        
		private int numberOfDaysStart;
		private int numberOfDaysEnd;
		private int numberOfStartsStart;
		private int numberOfStartsEnd;
		
		public void prepare() {
			if (numberOfDays != null) {
				String[] s = numberOfDays.split("-");
				if (s.length == 2) {
					numberOfDaysStart = Integer.parseInt(s[0]);
					numberOfDaysEnd = Integer.parseInt(s[1]);
				} else {
					numberOfDaysEnd = numberOfDaysStart = Integer.parseInt(s[0]);
				}
			}
			if (numberOfStarts != null) {
				String[] s = numberOfStarts.split("-");
				if (s.length == 2) {
					numberOfStartsStart = Integer.parseInt(s[0]);
					numberOfStartsEnd = Integer.parseInt(s[1]);
				} else {
					numberOfStartsEnd = numberOfStartsStart = Integer.parseInt(s[0]);
				}
			}
		}
        
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
			if (numberOfStartsStart != 0 && numberOfStartsEnd != 0) {
				filter += " Number of starts '" + numberOfStartsStart + "-" + numberOfStartsEnd + "' ";
			}
			if (numberOfDaysStart != 0 && numberOfDaysEnd != 0) {
				filter += " Number of days '" + numberOfDaysStart + "-" + numberOfDaysEnd + "' ";
			}
			
			if (startDate != null || endDate != null) {
				filter += String.format(" Date between %s and %s", 
						(startDate == null ? "-" : String.format("%1$tF %1$tR", startDate)),
						(endDate == null ? "-" : String.format("%1$tF %1$tR", endDate)));
			}
			if (appPackage != null) {
				filter += " appPackage '" + appPackage + "'";
			}
			if (appVersion != null) {
				filter += " AppVersion '" + appVersion + "'";
			}
			if (paid_features != null) {
				filter += " Features '" + paid_features + "'";
			}
			if (version != null) {
				filter += " Version in '" + version + "'";
			}
			return filter;

		}
        

		public boolean checkCondition(MessageParams params, IpLocationService locationService) {
			if (ip != null && !ip.contains(params.hostAddress)) {
				return false;
			}
			if (!checkActiveDate(params.now)) {
				return false;
			}
			if (country != null) {
				String cnt = locationService.getField(params.hostAddress, IpLocationService.COUNTRY_NAME);
				if (!cnt.equalsIgnoreCase(country)) {
					return false;
				}
			}
			if (numberOfDaysStart != 0 && numberOfDaysEnd != 0) {
				if (params.numberOfDays == null || params.numberOfDays.intValue() > numberOfDaysEnd
						|| params.numberOfDays.intValue() < numberOfDaysStart) {
					return false;
				}
			}

			if (numberOfStartsStart != 0 && numberOfStartsEnd != 0) {
				if (params.numberOfStarts == null || params.numberOfStarts.intValue() > numberOfStartsEnd
						|| params.numberOfStarts.intValue() < numberOfDaysStart) {
					return false;
				}
			}

			if (city != null) {
				String cnt = locationService.getField(params.hostAddress, IpLocationService.CITY);
				if (!cnt.equalsIgnoreCase(city)) {
					return false;
				}
			}
			if (this.os != null && this.os.length() > 0) {
				String osVersion = params.os != null && params.os.equals("ios") ? "ios" : "android";
				if (!osVersion.equals(this.os)) {
					return false;
				}
			}
			if (!Algorithms.isEmpty(this.paid_features)) {
				String[] conditions = this.paid_features.split(",");
				boolean allFailed = false;
				for (String condition : conditions) {
					String c = condition.trim();
					if (c.startsWith("-")) {
						// negative condition
						if (params.features.contains(c.substring(1))) {
							return false;
						}
					} else {
						if (!params.features.contains(c)) {
							allFailed = true;
						} else {
							allFailed = false;
						}
					}
				}
				if (allFailed) {
					return false;
				}
			}
			if (this.appPackage != null
					&& (params.appPackage == null || !params.appPackage.equalsIgnoreCase(this.appPackage))) {
				return false;
			}
			if (this.appVersion != null
					&& (params.appVersion == null || !params.appVersion.equalsIgnoreCase(this.appVersion))) {
				return false;
			}
			if (!checkVersion(params.version)) {
				return false;
			}

			if (this.lang != null && (params.lang == null || !this.lang.contains(params.lang))) {
				return false;
			}
			return true;
		}

		private boolean checkVersion(String clientVersion) {
			if (version == null) {
				return true;
			}
			if (Algorithms.isEmpty(clientVersion)) {
				return false;
			}
			try {
				if (version.startsWith(">=")) {
					return compareVersions(clientVersion, version.substring(2).trim()) >= 0;
				} else if (version.startsWith(">")) {
					return compareVersions(clientVersion, version.substring(1).trim()) > 0;
				} else if (version.startsWith("<=")) {
					return compareVersions(clientVersion, version.substring(2).trim()) <= 0;
				} else if (version.startsWith("<")) {
					return compareVersions(clientVersion, version.substring(1).trim()) < 0;
				}
				return clientVersion.startsWith(version);
			} catch (NumberFormatException e) {
				return false;
			}
		}

		private int compareVersions(String clientVersion, String requiredVersion) {
			if (requiredVersion.isEmpty()) {
				return 1;
			}

			// Android dev builds append APK_VERSION_SUFFIX, e.g. 5.5.0#99999M.
			int buildSuffix = clientVersion.indexOf('#');
			if (buildSuffix >= 0) {
				clientVersion = clientVersion.substring(0, buildSuffix);
			}

			String[] clientParts = clientVersion.split("\\.");
			String[] requiredParts = requiredVersion.split("\\.");

			int length = Math.max(clientParts.length, requiredParts.length);
			for (int i = 0; i < length; i++) {
				int clientPart = i < clientParts.length ? Integer.parseInt(clientParts[i]) : 0;
				int requiredPart = i < requiredParts.length ? Integer.parseInt(requiredParts[i]) : 0;
				int comparison = Integer.compare(clientPart, requiredPart);
				if (comparison != 0) {
					return comparison;
				}
			}
			return 0;
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
