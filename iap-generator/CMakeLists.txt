project(productizer)

if (CMAKE_SHARED_LIBS_ALLOWED_ON_TARGET)
	add_executable(productizer
		"main.cpp"
	)
	add_dependencies(productizer
		OsmAndCoreTools_shared
		OsmAndCore_ResourcesBundle_shared
	)
	target_link_libraries(productizer
		OsmAndCoreTools_shared
		OsmAndCore_ResourcesBundle_shared
	)
endif()

if (TARGET OsmAndCoreTools_static_standalone)
	add_executable(productizer_standalone
		"main.cpp"
	)
	add_dependencies(productizer_standalone
		OsmAndCoreTools_static_standalone
		OsmAndCore_ResourcesBundle_static
	)
	target_link_libraries(productizer_standalone
		OsmAndCoreTools_static_standalone
	)
	link_entire_static_library(productizer_standalone OsmAndCore_ResourcesBundle_static)
endif()
