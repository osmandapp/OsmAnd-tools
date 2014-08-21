project(verifier)

if (CMAKE_SHARED_LIBS_ALLOWED_ON_TARGET)
	add_executable(verifier
		"main.cpp"
	)
	add_dependencies(verifier
		OsmAndCoreTools_shared
	)
	target_link_libraries(verifier
		OsmAndCoreTools_shared
	)
endif()

if (CMAKE_STATIC_LIBS_ALLOWED_ON_TARGET)
	add_executable(verifier_standalone
		"main.cpp"
	)
	add_dependencies(verifier_standalone
		OsmAndCoreTools_static
	)
	target_link_libraries(verifier_standalone
		OsmAndCoreTools_static
	)
endif()
