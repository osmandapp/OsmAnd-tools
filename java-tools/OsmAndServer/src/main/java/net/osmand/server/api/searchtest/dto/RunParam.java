package net.osmand.server.api.searchtest.dto;

public record RunParam(boolean baseSearch, String locale, String northWest, String southEast, Double lat, Double lon) {
}
