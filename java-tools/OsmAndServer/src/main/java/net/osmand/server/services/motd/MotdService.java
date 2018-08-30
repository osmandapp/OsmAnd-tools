package net.osmand.server.services.motd;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

@Service
public class MotdService {
    private static final Log LOGGER = LogFactory.getLog(MotdService.class);
    private static final String MOTD_SETTINGS = "api/messages/motd_config.json";
    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm";

    @Value("${files.location}")
    private String websiteLocation;

    private final ObjectMapper mapper;

    public MotdService() {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(dateFormat);
        this.mapper = objectMapper;
    }

    private MotdSettings settings;

    public MotdMessage getMessage(String version, String os, HttpHeaders headers) throws IOException, ParseException {
        Date now = new Date();
        MotdMessage message = null;
        String hostAddress = getHostAddress(headers);
        for (DiscountSetting setting : settings.getDiscountSettings()) {
            if (setting.getDiscountCondition().checkCondition(now, hostAddress, version)) {
                String filename = setting.getMotdFileByPlatform(os);
                message = parseMotdMessageFile(websiteLocation.concat("api/messages/").concat(filename));
                message = modifyMessageIfNeeded(setting, message);
            }
        }
        return message;
    }

    public String updateSettings(List<String> errors) {
        MotdSettings motdSettings;
        String msg = "{\"status\": \"OK\", \"message\" : \"New configuration accepted.\"}";
        try {
            motdSettings = mapper.readValue(new File(websiteLocation.concat(MOTD_SETTINGS)), MotdSettings.class);
            this.settings = motdSettings;
        } catch (IOException ex) {
            errors.add("motd_config.json is invalid.");
            msg = String.format("{\"status\": \"FAILED\", \"message\": \"%s\"}", ex.getMessage());
            LOGGER.error(ex.getMessage(), ex);
        }
        return msg;
    }

    private String getHostAddress(HttpHeaders headers) {
       String address = headers.getFirst("X-Forwarded-For");
       if (address != null) {
           return address;
       }
       InetSocketAddress host = headers.getHost();
       return host.getAddress().toString();
    }

    private MotdMessage modifyMessageIfNeeded(DiscountSetting setting, MotdMessage message) {
        if (setting.isFieldsPresent()) {
            Map<String, String> fields = setting.getFields();
            message.setDescription(fields.get("description"));
            message.setMessage(fields.get("message"));
            message.setStartDate(fields.get("start"));
            message.setEndDate(fields.get("end"));
        }
        return message;
    }

    private MotdMessage parseMotdMessageFile(String filepath) throws IOException {
        return mapper.readValue(new File(filepath), MotdMessage.class);
    }

    public static class MotdSettings {

        @JsonProperty("settings")
        private List<DiscountSetting> discountSettings;

        public List<DiscountSetting> getDiscountSettings() {
            return discountSettings;
        }
    }

    private static class DiscountSetting {
        @JsonProperty("condition")
        private DiscountCondition discountCondition;
        private String file;
        @JsonProperty("ios_file")
        private String iosFile;
        private Map<String, String> fields;

        public DiscountCondition getDiscountCondition() {
            return discountCondition;
        }

        public String getFile() {
            return file;
        }

        public String getIosFile() {
            return iosFile;
        }

        public Map<String, String> getFields() {
            return fields;
        }

        public boolean isFilePresent() {
            return file != null;
        }

        public boolean isIosFilePresent() {
            return iosFile != null;
        }

        public boolean isFieldsPresent() {
            return fields != null;
        }

        public String getMotdFileByPlatform(String os) {
            if (os != null && os.equals("ios") && isIosFilePresent()) {
                return iosFile;
            }
            return file;
        }
    }

    private static class DiscountCondition {
        private String ip;
        @JsonProperty("start_date")
        private Date startDate;
        @JsonProperty("end_date")
        private Date endDate;
        private String version;

        private boolean isDiscountActive(Date now) {
            return now.after(getStartDate()) && now.before(getEndDate());
        }

        public String getIp() {
            return ip;
        }

        public Date getStartDate() {
            return startDate;
        }

        public Date getEndDate() {
            return endDate;
        }

        public String getVersion() {
            return version;
        }

        public boolean isIpPresent() {
            return ip != null;
        }

        public boolean isStartDatePresent() {
            return startDate != null;
        }

        public boolean isEndDatePresent() {
            return endDate != null;
        }

        public boolean isVersionPresent() {
            return version != null;
        }

        public boolean checkCondition(Date date, String hostAddress, String version) {
            boolean result = true;
            boolean anyCondition = false;
            if (isIpPresent()) {
                anyCondition = true;
                result &= getIp().equals(hostAddress);
            }
            if (isStartDatePresent() && isEndDatePresent() && isDiscountActive(date)) {
                anyCondition = true;
                result &= true;
            }
            if (isVersionPresent()) {
                anyCondition = true;
                result &= version != null && getVersion().startsWith(version);
            }
            return result && anyCondition;
        }
    }
}
