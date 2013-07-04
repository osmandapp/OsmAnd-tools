project(eyepiece)

add_executable(eyepiece
	"main.cpp"
)
add_dependencies(eyepiece
	OsmAndCoreUtils
)
target_link_libraries(eyepiece
	OsmAndCoreUtils
)
