package net.osmand.server.services.motd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class MotdMessage {
    @JsonProperty("message")
    private String message;
    @JsonProperty("description")
    private String description;
    @JsonProperty("start")
    private String startDate;
    @JsonProperty("end")
    private String endDate;
    @JsonProperty("show_start_frequency")
    private int showStartFrequency;
    @JsonProperty("show_day_frequency")
    private int showDayFrequency;
    @JsonProperty("max_total_show")
    private int maxTotalShow;
    @JsonProperty("icon")
    private String icon;
    @JsonProperty("url")
    private String url;
    @JsonProperty("url_1")
    private String urlOne;
    @JsonProperty("url_2")
    private String urlTwo;
    @JsonProperty("url_3")
    private String urlThree;
    @JsonProperty("in_app")
    private String inApp;
    @JsonProperty("purchased_in_apps")
    private List<String> purchasedInApps;
    @JsonProperty("application")
    private Map<String, Boolean> application;

    public void setMessage(String message) {
        this.message = message;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }
}
