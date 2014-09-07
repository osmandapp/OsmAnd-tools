project(voyager)

if (CMAKE_SHARED_LIBS_ALLOWED_ON_TARGET)
	add_executable(voyager
		"main.cpp"
	)
	add_dependencies(voyager
		OsmAndCoreTools_shared
		OsmAndCore_ResourcesBundle_shared
	)
	target_link_libraries(voyager
		OsmAndCoreTools_shared
		OsmAndCore_ResourcesBundle_shared
	)
endif()

if (CMAKE_STATIC_LIBS_ALLOWED_ON_TARGET)
	add_executable(voyager_standalone
		"main.cpp"
	)
	add_dependencies(voyager_standalone
		OsmAndCoreTools_static
		OsmAndCore_ResourcesBundle_static
	)
	target_link_libraries(voyager_standalone
		OsmAndCoreTools_static
		${CMAKE_WHOLE_ARCHIVE_BEGIN} OsmAndCore_ResourcesBundle_static ${CMAKE_WHOLE_ARCHIVE_END}
	)
endif()
