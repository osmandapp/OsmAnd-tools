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
find_package(Qt5Core REQUIRED)
qt5_use_modules(eyepiece Core)
