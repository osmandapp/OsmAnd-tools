package net.osmand.server.services.motd;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
public class MotdService {
    private static final Log LOGGER = LogFactory.getLog(MotdService.class);
    private static final String MOTD_SETTINGS = "api/messages/motd_config.json";

    @Value("${website.location}")
    private String websiteLocation;

    private final ObjectMapper mapper;

    private MotdSettings settings;

    @Autowired
    public MotdService(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String getMessage(String version, String os, HttpHeaders headers) {
        String message = null;
        String hostAddress = getHostAddress(headers);
        for (DiscountSetting setting : settings.getDiscountSettings()) {
            if (handleCondition(setting.getDiscountCondition(), version, hostAddress)) {
                return chooseFileByPlatform(setting, os);
            }
        }
        return message;
    }

    public void updateSettings(List<String> errors) {
        MotdSettings motdSettings;
        try {
            motdSettings = mapper.readValue(new File(websiteLocation.concat(MOTD_SETTINGS)), MotdSettings.class);
            this.settings = motdSettings;
        } catch (IOException ex) {
            errors.add(ex.getMessage());
            LOGGER.error(ex.getMessage(), ex);
        }
    }

    private String chooseFileByPlatform(DiscountSetting setting, String os) {
        if (os != null && os.equals("ios") && setting.isIosFilePresent()) {
            return setting.getIosFile();
        }
        return setting.getFile();
    }

    private String getHostAddress(HttpHeaders headers) {
       String address = headers.getFirst("X-Forwarded-For");
       if (address != null) {
           return address;
       }
       InetSocketAddress host = headers.getHost();
       return host.getAddress().toString();
    }

    private boolean handleCondition(DiscountCondition condition, String version, String hostAddress) {
        boolean result = true;
        boolean anyCondition = false;
        if (condition.isIpPresent()) {
            anyCondition = true;
            result &= condition.getIp().equals(hostAddress);
        }
        if (condition.isStartDatePresent() && condition.isEndDatePresent() && isDiscountActive(condition)) {
            anyCondition = true;
            result &= true;
        }
        if (condition.isVersionPresent()) {
            anyCondition = true;
            result &= version != null && condition.getVersion().startsWith(version);
        }
        return result && anyCondition;
    }

    private boolean isDiscountActive(DiscountCondition condition) {
        Date now = new Date();
        return now.after(condition.getStartDate()) && now.before(condition.getEndDate());
    }

    public static class MotdSettings {

        @JsonProperty("settings")
        private List<DiscountSetting> discountSettings;

        public List<DiscountSetting> getDiscountSettings() {
            return discountSettings;
        }

        public void setDiscountSettings(List<DiscountSetting> discountSettings) {
            this.discountSettings = discountSettings;
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

        public void setDiscountCondition(DiscountCondition discountCondition) {
            this.discountCondition = discountCondition;
        }

        public String getFile() {
            return file;
        }

        public void setFile(String file) {
            this.file = file;
        }

        public String getIosFile() {
            return iosFile;
        }

        public void setIosFile(String iosFile) {
            this.iosFile = iosFile;
        }

        public Map<String, String> getFields() {
            return fields;
        }

        public void setFields(Map<String, String> fields) {
            this.fields = fields;
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
    }

    private static class DiscountCondition {
        private String ip;
        @JsonProperty("start_date")
        private Date startDate;
        @JsonProperty("end_date")
        private Date endDate;
        private String version;

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public Date getStartDate() {
            return startDate;
        }

        public void setStartDate(Date startDate) {
            this.startDate = startDate;
        }

        public Date getEndDate() {
            return endDate;
        }

        public void setEndDate(Date endDate) {
            this.endDate = endDate;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
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
    }
}
