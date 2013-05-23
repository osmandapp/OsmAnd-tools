cmake_minimum_required(VERSION 2.8.8)
project(eyepiece)

include_directories(AFTER SYSTEM
	"${OSMAND_ROOT}/core/externals/protobuf/upstream.patched/src"
	"${OSMAND_ROOT}/core/include"
	"${OSMAND_ROOT}/core/protos"
	"${OSMAND_ROOT}/core/native/include"
	"${OSMAND_ROOT}/core/utils"
)

find_package(Qt5Core REQUIRED)
add_executable(eyepiece
	"main.cpp"
)
target_link_libraries(eyepiece OsmAndCoreUtils OsmAndCore)
qt5_use_modules(eyepiece Core)
