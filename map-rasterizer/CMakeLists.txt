project(eyepiece)

if(CMAKE_SHARED_LIBS_ALLOWED_ON_TARGET)
	add_executable(eyepiece
		"main.cpp"
	)
	add_dependencies(eyepiece
		OsmAndCoreUtils_shared
	)
	target_link_libraries(eyepiece
		OsmAndCoreUtils_shared
	)
endif()

if(CMAKE_STATIC_LIBS_ALLOWED_ON_TARGET)
	add_executable(eyepiece_standalone
		"main.cpp"
	)
	add_dependencies(eyepiece_standalone
		OsmAndCoreUtils_static
	)
	target_link_libraries(eyepiece_standalone
		OsmAndCoreUtils_static
	)
endif()
