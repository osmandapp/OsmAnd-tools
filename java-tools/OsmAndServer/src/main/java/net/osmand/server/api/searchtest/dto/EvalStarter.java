package net.osmand.server.api.searchtest.dto;

public record EvalStarter(String functionName, String[] columns, String[] paramValues,
						  boolean baseSearch, String locale, String northWest, String southEast) {
}
