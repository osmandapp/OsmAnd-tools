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
find_package(Qt5Core REQUIRED)
qt5_use_modules(inspector Core)
