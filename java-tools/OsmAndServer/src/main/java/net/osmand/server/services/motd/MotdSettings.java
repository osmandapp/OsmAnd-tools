package net.osmand.server.services.motd;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MotdSettings {
    @JsonProperty("start_date")
    private String startDate;

    @JsonProperty("end_date")
    private String endDate;

    @JsonProperty("test_ip")
    private String testIpAddress;

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

    public String getTestIpAddress() {
        return testIpAddress;
    }

    public void setTestIpAddress(String testIpAddress) {
        this.testIpAddress = testIpAddress;
    }
}
