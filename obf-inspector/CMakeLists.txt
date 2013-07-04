project(inspector)

add_executable(inspector
	"main.cpp"
)
add_dependencies(inspector
	OsmAndCoreUtils
)
target_link_libraries(inspector
	OsmAndCoreUtils
)
