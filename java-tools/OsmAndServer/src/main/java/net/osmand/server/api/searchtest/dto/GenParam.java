package net.osmand.server.api.searchtest.dto;

public record GenParam(String name, String labels, Function[] functions, String functionName, String[] columns, String[] paramValues) {
}
