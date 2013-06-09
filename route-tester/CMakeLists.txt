cmake_minimum_required(VERSION 2.8.8)
project(voyager)

include_directories(AFTER SYSTEM
	"${OSMAND_ROOT}/core/externals/protobuf/upstream.patched/src"
	"${OSMAND_ROOT}/core/include"
	"${OSMAND_ROOT}/core/protos"
	"${OSMAND_ROOT}/core/native/include"
	"${OSMAND_ROOT}/core/utils"
)

find_package(Qt5Core REQUIRED)
add_executable(voyager
	"main.cpp"
)
add_dependencies(voyager OsmAndCoreUtils OsmAndCore)
target_link_libraries(voyager OsmAndCoreUtils OsmAndCore)
qt5_use_modules(voyager Core)
