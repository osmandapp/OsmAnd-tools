project(bird)

add_subdirectory("${OSMAND_ROOT}/tools/map-viewer/externals/freeglut" "tools/map-viewer/externals/freeglut")

if(CMAKE_SHARED_LIBS_ALLOWED_ON_TARGET)
	add_executable(bird
		"main.cpp"
	)

	add_dependencies(bird
		OsmAndCore_shared
		freeglut_static
	)
	target_link_libraries(bird
		OsmAndCore_shared
		freeglut_static
	)
endif()

if(CMAKE_STATIC_LIBS_ALLOWED_ON_TARGET)
	add_executable(bird_standalone
		"main.cpp"
	)

	add_dependencies(bird_standalone
		OsmAndCore_static
		freeglut_static
	)
	target_link_libraries(bird_standalone
		OsmAndCore_static
		freeglut_static
	)
endif()
