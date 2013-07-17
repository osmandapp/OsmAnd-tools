project(bird)

find_package(GLUT)
if(NOT GLUT_FOUND)
	add_subdirectory("${OSMAND_ROOT}/tools/map-viewer/externals/freeglut" "tools/map-viewer/externals/freeglut")
endif()

if(CMAKE_SHARED_LIBS_ALLOWED_ON_TARGET)
	add_executable(bird
		"main.cpp"
	)

	if(NOT GLUT_FOUND)
		add_dependencies(bird
			OsmAndCore_shared
			freeglut_static
		)
		target_link_libraries(bird
			OsmAndCore_shared
			freeglut_static
		)
	else()
		add_dependencies(bird
			OsmAndCore_shared
		)
		target_link_libraries(bird
			OsmAndCore_shared
			${GLUT_LIBRARY}
		)

		include_directories(${GLUT_INCLUDE_DIRS})
		link_directories(${GLUT_LIBRARY_DIRS})
		add_definitions(${GLUT_DEFINITIONS})
	endif()
endif()

if(CMAKE_STATIC_LIBS_ALLOWED_ON_TARGET)
	add_executable(bird_standalone
		"main.cpp"
	)

	if(NOT GLUT_FOUND)
		add_dependencies(bird_standalone
			OsmAndCore_static
			freeglut_static
		)
		target_link_libraries(bird_standalone
			OsmAndCore_static
			freeglut_static
		)
	else()
		add_dependencies(bird_standalone
			OsmAndCore_static
		)
		target_link_libraries(bird_standalone
			OsmAndCore_shared
			${GLUT_LIBRARY}
		)

		include_directories(${GLUT_INCLUDE_DIRS})
		link_directories(${GLUT_LIBRARY_DIRS})
		add_definitions(${GLUT_DEFINITIONS})
	endif()
endif()
