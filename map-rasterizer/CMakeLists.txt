project(eyepiece)

if (CMAKE_SHARED_LIBS_ALLOWED_ON_TARGET)
	add_executable(eyepiece
		"main.cpp"
	)
	add_dependencies(eyepiece
		OsmAndCoreTools_shared
		OsmAndCore_ResourcesBundle_shared
	)
	target_link_libraries(eyepiece
		OsmAndCoreTools_shared
		OsmAndCore_ResourcesBundle_shared
	)
endif()

if (CMAKE_STATIC_LIBS_ALLOWED_ON_TARGET)
	add_executable(eyepiece_standalone
		"main.cpp"
	)
	add_dependencies(eyepiece_standalone
		OsmAndCoreTools_static
		OsmAndCore_ResourcesBundle_static
	)
	target_link_libraries(eyepiece_standalone
		OsmAndCoreTools_static
		${CMAKE_WHOLE_ARCHIVE_BEGIN} OsmAndCore_ResourcesBundle_static ${CMAKE_WHOLE_ARCHIVE_END}
	)
endif()
