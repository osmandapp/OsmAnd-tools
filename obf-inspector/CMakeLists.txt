project(inspector)

if (CMAKE_SHARED_LIBS_ALLOWED_ON_TARGET)
	add_executable(inspector
		"main.cpp"
	)
	add_dependencies(inspector
		OsmAndCoreTools_shared
	)
	target_link_libraries(inspector
		OsmAndCoreTools_shared
	)
endif()

if (CMAKE_STATIC_LIBS_ALLOWED_ON_TARGET)
	add_executable(inspector_standalone
		"main.cpp"
	)
	add_dependencies(inspector_standalone
		OsmAndCoreTools_static
	)
	target_link_libraries(inspector_standalone
		OsmAndCoreTools_static
	)
endif()
