project(styler)

if (CMAKE_SHARED_LIBS_ALLOWED_ON_TARGET)
	add_executable(styler
		"main.cpp"
	)
	add_dependencies(styler
		OsmAndCoreTools_shared
		OsmAndCore_ResourcesBundle_shared
	)
	target_link_libraries(styler
		OsmAndCoreTools_shared
		OsmAndCore_ResourcesBundle_shared
	)
endif()

if (TARGET OsmAndCoreTools_static_standalone)
	add_executable(styler_standalone
		"main.cpp"
	)
	add_dependencies(styler_standalone
		OsmAndCoreTools_static_standalone
		OsmAndCore_ResourcesBundle_static
	)
	target_link_libraries(styler_standalone
		OsmAndCoreTools_static_standalone
	)
	link_entire_static_library(styler_standalone OsmAndCore_ResourcesBundle_static)
endif()
