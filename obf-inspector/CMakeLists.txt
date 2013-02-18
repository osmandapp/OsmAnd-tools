cmake_minimum_required(VERSION 2.8.8)
project(inspector)

add_executable(inspector
	"main.cpp"
)
target_link_libraries(inspector OsmAndCore)
