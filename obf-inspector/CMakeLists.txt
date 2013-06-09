cmake_minimum_required(VERSION 2.8.8)
project(inspector)

include_directories(AFTER SYSTEM
	"${OSMAND_ROOT}/core/externals/protobuf/upstream.patched/src"
	"${OSMAND_ROOT}/core/include"
	"${OSMAND_ROOT}/core/protos"
	"${OSMAND_ROOT}/core/native/include"
	"${OSMAND_ROOT}/core/utils"
)

find_package(Qt5Core REQUIRED)
add_executable(inspector
	"main.cpp"
)
add_dependencies(inspector OsmAndCoreUtils OsmAndCore)
target_link_libraries(inspector OsmAndCoreUtils OsmAndCore)
qt5_use_modules(inspector Core)
