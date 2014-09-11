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

if (TARGET OsmAndCoreTools_static_standalone)
	add_executable(voyager_standalone
		"main.cpp"
	)
	add_dependencies(voyager_standalone
		OsmAndCoreTools_static_standalone
		OsmAndCore_ResourcesBundle_static
	)
	target_link_libraries(voyager_standalone
		OsmAndCoreTools_static_standalone
	)
	link_entire_static_library(voyager_standalone OsmAndCore_ResourcesBundle_static)
endif()
