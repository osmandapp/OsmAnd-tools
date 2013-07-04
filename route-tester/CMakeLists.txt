project(voyager)

add_executable(voyager
	"main.cpp"
)
add_dependencies(voyager
	OsmAndCoreUtils
)
target_link_libraries(voyager
	OsmAndCoreUtils
)
