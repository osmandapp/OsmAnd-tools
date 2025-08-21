package net.osmand.server.api.searchtest.dto;

public record GenParam(
        String name,
        String labels,
        // New fields to explicitly support 2 functions
        String selectFun,
        String whereFun,
        String[] columns,
        // Legacy single params array (maps to select function when used)
        String[] paramValues,
        // New fields: separate params for select/where
        String[] selectParamValues,
        String[] whereParamValues
) {
}
