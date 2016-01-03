# OBF inspector tool
add_subdirectory("${OSMAND_ROOT}/tools/cpp-tools/obf-inspector" "tools/cpp-tools/obf-inspector")

# Route tester
add_subdirectory("${OSMAND_ROOT}/tools/cpp-tools/route-tester" "tools/cpp-tools/route-tester")

# Map rasterizer
add_subdirectory("${OSMAND_ROOT}/tools/cpp-tools/map-rasterizer" "tools/cpp-tools/map-rasterizer")

# Map style evaluator
add_subdirectory("${OSMAND_ROOT}/tools/cpp-tools/style-evaluator" "tools/cpp-tools/style-evaluator")

# Map style compiler
add_subdirectory("${OSMAND_ROOT}/tools/cpp-tools/style-compiler" "tools/cpp-tools/style-compiler")

# 3D Map renderer (development)
add_subdirectory("${OSMAND_ROOT}/tools/cpp-tools/map-viewer" "tools/cpp-tools/map-viewer")
