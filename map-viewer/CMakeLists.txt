project(bird)

add_executable(bird
	"main.cpp"
)

add_subdirectory("${OSMAND_ROOT}/tools/map-viewer/externals/freeglut" "tools/map-viewer/externals/freeglut")

add_dependencies(bird
	OsmAndCore
	freeglut_static
)
target_link_libraries(bird
	OsmAndCore
	freeglut_static
)
