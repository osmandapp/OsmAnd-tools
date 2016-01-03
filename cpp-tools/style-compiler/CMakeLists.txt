project(cstyle)

if (CMAKE_SHARED_LIBS_ALLOWED_ON_TARGET)
	add_executable(cstyle
		"main.cpp"
	)
	add_dependencies(cstyle
		OsmAndCoreTools_shared
		OsmAndCore_ResourcesBundle_shared
	)
	target_link_libraries(cstyle
		OsmAndCoreTools_shared
		OsmAndCore_ResourcesBundle_shared
	)
endif()

if (TARGET OsmAndCoreTools_static_standalone)
	add_executable(cstyle_standalone
		"main.cpp"
	)
	add_dependencies(cstyle_standalone
		OsmAndCoreTools_static_standalone
		OsmAndCore_ResourcesBundle_static
	)
	target_link_libraries(cstyle_standalone
		OsmAndCoreTools_static_standalone
	)
	link_entire_static_library(cstyle_standalone OsmAndCore_ResourcesBundle_static)
endif()
