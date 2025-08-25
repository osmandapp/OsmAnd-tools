package net.osmand.server.api.searchtest.dto;

public record GenParam(
		String name,
		String labels,
		// New fields to explicitly support 2 functions
		String selectFun,
		String whereFun,
		String[] columns,
		String[] selectParamValues,
		String[] whereParamValues) {}
