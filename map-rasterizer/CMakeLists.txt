project(eyepiece)

if (CMAKE_SHARED_LIBS_ALLOWED_ON_TARGET)
	add_executable(eyepiece
		"main.cpp"
	)
	add_dependencies(eyepiece
		OsmAndCoreTools_shared
	)
	target_link_libraries(eyepiece
		OsmAndCoreTools_shared
	)
endif()

if (CMAKE_STATIC_LIBS_ALLOWED_ON_TARGET)
	add_executable(eyepiece_standalone
		"main.cpp"
	)
	add_dependencies(eyepiece_standalone
		OsmAndCoreTools_static
	)
	target_link_libraries(eyepiece_standalone
		OsmAndCoreTools_static
	)
endif()
