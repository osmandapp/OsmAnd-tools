project(bird)

add_executable(bird
	"main.cpp"
)

add_subdirectory("${OSMAND_ROOT}/tools/map-viewer/externals/freeglut" "tools/map-viewer/externals/freeglut")
find_package(Qt5Core REQUIRED)
qt5_use_modules(bird Core)

add_dependencies(bird
	OsmAndCore
	freeglut_static
)
target_link_libraries(bird
	OsmAndCore
	freeglut_static
)
