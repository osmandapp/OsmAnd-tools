package net.osmand.server.services.motd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
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

    @Value("${motd.settings}")
    private String motdSettingsLocation;

    private MotdSettings settings;

    @Autowired
    public MotdService(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String getMessage(String version, String os, HttpHeaders headers) {
        String message = null;
        String hostAddress = getHostAddress(headers);
        for (DiscountSetting setting : settings.getDiscountSettings()) {
            message = handleCondition(setting, os, version, hostAddress);
            if (message != null) {
                break;
            }
        }
        return message;
    }

    public void updateSettings(List<Exception> errors) {
        MotdSettings motdSettings;
        try {
            motdSettings = mapper.readValue(new File(motdSettingsLocation), MotdSettings.class);
            this.settings = motdSettings;
        } catch (IOException ex) {
            errors.add(ex);
            LOGGER.error(ex.getMessage(), ex);
        }
    }

    private String getHostAddress(HttpHeaders headers) {
       String address = headers.getFirst("X-Forwarded-For");
       if (address != null) {
           return address;
       }
       InetSocketAddress host = headers.getHost();
       return host.getAddress().toString();
    }

    private String handleCondition(DiscountSetting setting, String os, String version, String hostAddress) {
        DiscountCondition condition = setting.getDiscountCondition();
        if (checkIpAddressCondition(condition, hostAddress)) {
            return setting.getFile();
        } else if (isDiscountActive(setting)) {
            if (os != null && os.equals("ios")) {
                return setting.getIosFile();
            }
            return setting.getFile();
        } else if (checkVersionCondition(condition, version)) {
            return setting.getFile();
        }
        return null;
    }

    private boolean isDiscountActive(DiscountSetting setting) {
        DiscountCondition discountCondition = setting.getDiscountCondition();
        Date now = new Date();
        Date startDate = discountCondition.getStartDate();
        Date endDate = discountCondition.getEndDate();
        return startDate != null && endDate != null && now.after(startDate) && now.before(endDate);
    }

    private boolean checkIpAddressCondition(DiscountCondition condition, String expectedIpAddress) {
        return condition.getIp() != null && condition.getIp().equals(expectedIpAddress);
    }

    private boolean checkVersionCondition(DiscountCondition condition, String expectedVersion) {
        return condition.getVersion() != null && expectedVersion != null && condition.getVersion().startsWith(expectedVersion);
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

    @JsonIgnoreProperties(ignoreUnknown = true)
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

    @JsonDeserialize(using = DiscountConditionDeserializer.class)
    private static class DiscountCondition {

        private String ip;

        private Date startDate;

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
    }

    @Component
    private static class DiscountConditionDeserializer extends JsonDeserializer<DiscountCondition> {

        private static final String START_DATE = "start_date";
        private static final String END_DATE = "end_date";
        private static final String IP_ADDR = "ip";
        private static final String VERSION = "version";

        private final SimpleDateFormat dateFormat;

        @Autowired
        public DiscountConditionDeserializer(SimpleDateFormat dateFormat) {
            this.dateFormat = dateFormat;
        }

        private Date parseDate(JsonNode dateNode) throws ParseException {
            if (dateNode == null) {
                return null;
            }
            return dateFormat.parse(dateNode.asText());
        }

        private String parseNodeOrNull(JsonNode node) {
            if (node == null) {
                return null;
            }
            return node.asText();
        }

        @Override
        public DiscountCondition deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            ObjectCodec oc = p.getCodec();
            JsonNode node = oc.readTree(p);
            try {
                Date startDate = parseDate(node.get(START_DATE));
                Date endDate = parseDate(node.get(END_DATE));
                String ipAddr = parseNodeOrNull(node.get(IP_ADDR));
                String version = parseNodeOrNull(node.get(VERSION));
                DiscountCondition ddc = new DiscountCondition();
                ddc.setStartDate(startDate);
                ddc.setEndDate(endDate);
                ddc.setIp(ipAddr);
                ddc.setVersion(version);
                return ddc;
            } catch (ParseException ex) {
                throw new DiscountConditionJsonProcessingException(ex.getMessage(), ex);
            }
        }
    }

    private static class DiscountConditionJsonProcessingException extends JsonProcessingException {

        public DiscountConditionJsonProcessingException(String msg, Throwable rootCause) {
            super(msg, rootCause);
        }
    }
}
