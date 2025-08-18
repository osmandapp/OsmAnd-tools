package net.osmand.server.api.searchtest.dto;

public record RunStarter(String name, String functionName, String[] columns, String[] paramValues,
						 boolean baseSearch, String locale, String northWest, String southEast, Double lat, Double lon) {
}
