package net.osmand.server.services.motd;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
public class MotdService {
    private static final Log LOGGER = LogFactory.getLog(MotdService.class);

    private final ObjectMapper mapper;
    private final SimpleDateFormat dateFormat;

    @Value("${motd.settings}")
    private String motdSettingsLocation;

    private MotdSettings settings;

    @Autowired
    public MotdService(ObjectMapper mapper, SimpleDateFormat dateFormat) {
        this.mapper = mapper;
        this.dateFormat = dateFormat;

    }

    public String getMessage(String version, String os, HttpHeaders headers) {
        updateSettings();
        String message = null;
        String hostAddress = getHostAddress(headers);
        for (DiscountSetting setting : settings.getDiscountSettings()) {
            DiscountCondition condition = setting.getDiscountCondition();
            if (condition instanceof IpDiscountCondition) {
                message = handleIpCondition(setting, hostAddress, message);
            } else if (condition instanceof DateDiscountCondition) {
                message = handleDateCondition(setting, os, message);
            } else if (condition instanceof VersionDiscountCondition) {
                message = handleVersionCondition(setting, version, message);
            }
        }
        return message;
    }

    public String updateSettings() {
        MotdSettings motdSettings;
        try {
            motdSettings = mapper.readValue(new File(motdSettingsLocation), MotdSettings.class);
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new CannotUpdateMotdSettingsException(ex);
        }
        synchronized (this) {
            this.settings = motdSettings;
        }
        return "{\"type\": \"success\", \"status\": \"OK\"}";
    }

    private String getHostAddress(HttpHeaders headers) {
       String address = headers.getFirst("X-Forwarded-For");
       if (address != null) {
           return address;
       }
       InetSocketAddress host = headers.getHost();
       return host.getAddress().toString();
    }

    private String handleIpCondition(DiscountSetting setting, String hostIp, String message) {
        if (message != null) {
            return message;
        }
        IpDiscountCondition ipCond = (IpDiscountCondition) setting.getDiscountCondition();
        if (ipCond.getIp().equals(hostIp)) {
            return setting.getFile();
        }
        return null;
    }

    private String handleDateCondition(DiscountSetting setting, String os, String message) {
        if (message != null) {
            return message;
        }
        if (!isDiscountActive(setting)) {
            return message;
        }
        if (os != null && os.equals("ios")) {
            return setting.getIosFile();
        }
        return setting.getFile();

    }

    private String handleVersionCondition(DiscountSetting setting, String version, String message) {
        if (message != null) {
            return message;
        }
        VersionDiscountCondition versionCondition = (VersionDiscountCondition) setting.getDiscountCondition();
        if (version != null && version.startsWith(versionCondition.getVersion())) {
            return setting.getFile();
        }
        return null;
    }

    private boolean isDiscountActive(DiscountSetting setting) {
        DateDiscountCondition dateCondition = (DateDiscountCondition) setting.getDiscountCondition();
        Date now = new Date();
        Date startDate;
        Date endDate;
        try {
            startDate = dateFormat.parse(dateCondition.getStartDate());
            endDate = dateFormat.parse(dateCondition.getEndDate());
        } catch (ParseException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new DiscountDatesParseException(ex);
        }
        return now.after(startDate) && now.before(endDate);
    }


    public static class CannotUpdateMotdSettingsException extends RuntimeException {

        public CannotUpdateMotdSettingsException() {}

        public CannotUpdateMotdSettingsException(String s) {
            super(s);
        }

        public CannotUpdateMotdSettingsException(Throwable throwable) {
            super(throwable);
        }
    }

    public static class DiscountDatesParseException extends RuntimeException {
        public DiscountDatesParseException() {
        }

        public DiscountDatesParseException(String s) {
            super(s);
        }

        public DiscountDatesParseException(Throwable throwable) {
            super(throwable);
        }
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
    }

    @JsonTypeInfo(property = "type", use = Id.NAME)
    @JsonSubTypes(
            {@Type(value = IpDiscountCondition.class, name = "ip"),
            @Type(value = DateDiscountCondition.class, name = "date"),
            @Type(value = VersionDiscountCondition.class, name = "version")})
    private abstract static class DiscountCondition {
        private String type;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    private static class IpDiscountCondition extends DiscountCondition {
        private String ip;

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }
    }

    private static class DateDiscountCondition extends DiscountCondition {
        @JsonProperty("start_date")
        private String startDate;
        @JsonProperty("end_date")
        private String endDate;

        public String getStartDate() {
            return startDate;
        }

        public void setStartDate(String startDate) {
            this.startDate = startDate;
        }

        public String getEndDate() {
            return endDate;
        }

        public void setEndDate(String endDate) {
            this.endDate = endDate;
        }
    }

    private static class VersionDiscountCondition extends DiscountCondition {
        private String version;

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }
    }
}
