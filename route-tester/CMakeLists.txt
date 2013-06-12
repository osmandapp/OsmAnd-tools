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
find_package(Qt5Core REQUIRED)
qt5_use_modules(voyager Core)
