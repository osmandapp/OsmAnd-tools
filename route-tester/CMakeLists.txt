project(voyager)

if (CMAKE_SHARED_LIBS_ALLOWED_ON_TARGET)
	add_executable(voyager
		"main.cpp"
	)
	add_dependencies(voyager
		OsmAndCoreUtils_shared
	)
	target_link_libraries(voyager
		OsmAndCoreUtils_shared
	)
endif()

if (CMAKE_STATIC_LIBS_ALLOWED_ON_TARGET)
	add_executable(voyager_standalone
		"main.cpp"
	)
	add_dependencies(voyager_standalone
		OsmAndCoreUtils_static
	)
	target_link_libraries(voyager_standalone
		OsmAndCoreUtils_static
	)
endif()
