# OBF inspector tool
add_subdirectory("${OSMAND_ROOT}/tools/obf-inspector" "tools/obf-inspector")
add_dependencies(inspector OsmAndCore OsmAndCoreUtils)

# Route tester
add_subdirectory("${OSMAND_ROOT}/tools/route-tester" "tools/route-tester")
add_dependencies(voyager OsmAndCore OsmAndCoreUtils)